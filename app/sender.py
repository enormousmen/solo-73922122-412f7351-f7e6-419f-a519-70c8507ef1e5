"""发送端：把图块经本地 UDP 中继分段发给接收端。

行为规则：
  - 初始按 1200 字节的 UDP 载荷上限发包（含 32 字节协议头）；
  - 同一段连续两次等不到确认 -> 降到 600 字节，只把尚未确认的部分重新分段；
  - 600 字节档下同一段连续两次超时 -> 传输失败；
  - 停等式：一次只发一段，收到覆盖该段的确认才推进。

完成判定（关键）：
  - 分片确认只代表“该段被某个接收进程写过”，不代表接收端存有整块图块。
    接收进程重启后，重启前后两个进程各自回执的分片可以拼起来覆盖整块，
    但没有任何一个进程持有完整图块。
  - 因此所有分片确认覆盖整块后，发送端进入“等待收齐回执”阶段：反复发送
    一小段探测数据，直到收到接收端发来的收齐回执（magic CPL1，total 匹配）
    才判 done。等待时间有上限（complete_wait_s），超时判 failed，并明确说明
    未能确认收齐。
  - 若接收端已收齐、只是收齐回执丢了一份，探测会促使它重发收齐回执，
    从而确认成功并允许下载。
"""
import os
import socket
import threading
import time
import uuid

from .intervals import IntervalSet
from .protocol import HDR_DATA, MAX_DATAGRAM, pack_data, unpack_ack, unpack_complete

LARGE_LIMIT = 1200  # 初始 UDP 载荷上限（字节）
SMALL_LIMIT = 600   # 降档后的 UDP 载荷上限（字节）
MAX_TIMEOUTS = 2    # 同一段连续超时次数阈值
COMPLETE_WAIT_SEC = float(os.environ.get("COMPLETE_WAIT_SEC", "3.0"))


class TransferState:
    """一次传输的共享状态，供 API 查询线程读取。"""

    def __init__(self, tile_id: str, total: int, complete_wait_s: float = COMPLETE_WAIT_SEC):
        self.tile_id = tile_id
        self.total = total
        self.status = "transferring"  # transferring | done | failed
        self.send_limit = LARGE_LIMIT  # 当前发包上限（UDP 载荷字节数）
        self.timeouts = 0              # 累计超时次数
        self.downgrades = 0            # 降档次数
        self.datagrams_sent = 0        # 累计发出的数据包数（含重发）
        self.probes_sent = 0           # 等待收齐阶段发出的探测包数
        self.complete_receipts = 0     # 收到的收齐回执数
        self.awaiting_complete = False # 分片回执已覆盖整块、正等收齐回执
        self.complete_confirmed = False
        self.complete_wait_s = complete_wait_s
        self.error = None
        self._lock = threading.Lock()

    def mark(self, **kwargs):
        with self._lock:
            for key, val in kwargs.items():
                setattr(self, key, val)

    def incr(self, field: str, delta: int = 1):
        with self._lock:
            setattr(self, field, getattr(self, field) + delta)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "tile_id": self.tile_id,
                "state": self.status,
                "send_limit": self.send_limit,
                "total": self.total,
                "timeouts": self.timeouts,
                "downgrades": self.downgrades,
                "datagrams_sent": self.datagrams_sent,
                "probes_sent": self.probes_sent,
                "complete_receipts": self.complete_receipts,
                "awaiting_complete": self.awaiting_complete,
                "complete_confirmed": self.complete_confirmed,
                "complete_wait_sec": self.complete_wait_s,
                "error": self.error,
            }


def _plan_segments(unacked_ranges, chunk_size: int, next_seq: int):
    """把未确认的字节区间切成不超过 chunk_size 的段，返回 [(offset, len, seq)], next_seq。"""
    segments = []
    seq = next_seq
    for start, end in unacked_ranges:
        off = start
        while off < end:
            ln = min(chunk_size, end - off)
            segments.append((off, ln, seq))
            seq += 1
            off += ln
    return segments, seq


def _process_receipt(state: TransferState, acked: IntervalSet, datagram: bytes,
                     tile_uuid: uuid.UUID, total: int):
    """处理一个回包：分片确认写入 acked；收齐回执返回 True。

    返回 True 仅当该包是给本图块的、total 匹配的收齐回执。
    """
    complete = unpack_complete(datagram)
    if complete is not None:
        tid, ctotal, _seq = complete
        if tid == tile_uuid and ctotal == total:
            state.incr("complete_receipts")
            return True
        return False
    ack = unpack_ack(datagram)
    if ack is None:
        return False
    tid, ack_off, ack_len, _ack_seq = ack
    if tid != tile_uuid:
        return False
    acked.add(ack_off, ack_off + ack_len)
    return False


def run_transfer(state: TransferState, data: bytes, relay_addr, timeout_s: float,
                 complete_wait_s: float = None):
    """阻塞式传输循环，在后台线程中运行；结果写回 state。"""
    total = len(data)
    tile_uuid = uuid.UUID(state.tile_id)
    complete_wait_s = state.complete_wait_s if complete_wait_s is None else complete_wait_s
    acked = IntervalSet()
    limit = LARGE_LIMIT
    seq_counter = 0
    segments, seq_counter = _plan_segments(
        acked.complement(total), limit - HDR_DATA.size, seq_counter)
    idx = 0
    consecutive_timeouts = 0

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(timeout_s)
        sock.connect(relay_addr)  # 只接受来自中继的回包

        def finish_done():
            state.mark(status="done", awaiting_complete=False, complete_confirmed=True)

        while idx < len(segments):
            off, ln, seq = segments[idx]
            if acked.covers(off, off + ln):
                # 迟到的确认已覆盖该段，无需再发
                idx += 1
                consecutive_timeouts = 0
                continue
            sock.send(pack_data(tile_uuid, off, total, seq, data[off:off + ln]))
            state.incr("datagrams_sent")
            while True:
                try:
                    datagram = sock.recv(MAX_DATAGRAM)
                except socket.timeout:
                    state.incr("timeouts")
                    consecutive_timeouts += 1
                    if consecutive_timeouts >= MAX_TIMEOUTS:
                        if limit > SMALL_LIMIT:
                            # 降档：只把尚未确认的部分按小档重新分段
                            limit = SMALL_LIMIT
                            state.mark(send_limit=limit)
                            state.incr("downgrades")
                            segments, seq_counter = _plan_segments(
                                acked.complement(total), limit - HDR_DATA.size, seq_counter)
                            idx = 0
                            consecutive_timeouts = 0
                        else:
                            state.mark(
                                status="failed",
                                error=(f"segment at offset {off} timed out {MAX_TIMEOUTS}x "
                                       f"at {limit}-byte tier"),
                            )
                            return
                    break  # 重发当前段，或按新分段表从头发送
                if _process_receipt(state, acked, datagram, tile_uuid, total):
                    finish_done()
                    return
                if acked.covers(off, off + ln):
                    idx += 1
                    consecutive_timeouts = 0
                    break
                # 否则是迟到/重复的确认，继续等当前段的确认

        # 所有分片都被确认过，但分片确认可能来自重启前后的不同接收进程。
        # 必须等到接收端自己发来的收齐回执，否则不能算成功。
        state.mark(awaiting_complete=True)
        deadline = time.monotonic() + complete_wait_s
        probe_len = min(total, SMALL_LIMIT - HDR_DATA.size)
        probe_off = max(0, total - probe_len)
        probe_payload = data[probe_off:probe_off + probe_len]
        while True:
            if time.monotonic() >= deadline:
                state.mark(
                    status="failed",
                    awaiting_complete=False,
                    error=(f"receiver did not confirm the whole tile within "
                           f"{complete_wait_s:g}s after per-segment receipts covered "
                           f"all {total} bytes; no receiving process holds the "
                           f"complete tile (segments may span receiver restarts)"),
                )
                return
            sock.send(pack_data(tile_uuid, probe_off, total, seq_counter, probe_payload))
            seq_counter += 1
            state.incr("probes_sent")
            try:
                while True:
                    datagram = sock.recv(MAX_DATAGRAM)
                    if _process_receipt(state, acked, datagram, tile_uuid, total):
                        finish_done()
                        return
                    # 探测段带来的分片确认（重复/重叠）不改变结论，继续等
            except socket.timeout:
                continue
    finally:
        sock.close()
