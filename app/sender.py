"""发送端：把图块经本地 UDP 中继分段发给接收端。

行为规则：
  - 初始按 1200 字节的 UDP 载荷上限发包（含 32 字节协议头）；
  - 同一段连续两次等不到确认 -> 降到 600 字节，只把尚未确认的部分重新分段；
  - 600 字节档下同一段连续两次超时 -> 传输失败；
  - 停等式：一次只发一段，收到覆盖该段的确认才推进。
"""
import socket
import threading
import uuid

from .intervals import IntervalSet
from .protocol import HDR_DATA, MAX_DATAGRAM, pack_data, unpack_ack

LARGE_LIMIT = 1200  # 初始 UDP 载荷上限（字节）
SMALL_LIMIT = 600   # 降档后的 UDP 载荷上限（字节）
MAX_TIMEOUTS = 2    # 同一段连续超时次数阈值


class TransferState:
    """一次传输的共享状态，供 API 查询线程读取。"""

    def __init__(self, tile_id: str, total: int):
        self.tile_id = tile_id
        self.total = total
        self.status = "transferring"  # transferring | done | failed
        self.send_limit = LARGE_LIMIT  # 当前发包上限（UDP 载荷字节数）
        self.timeouts = 0              # 累计超时次数
        self.downgrades = 0            # 降档次数
        self.datagrams_sent = 0        # 累计发出的数据包数（含重发）
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


def run_transfer(state: TransferState, data: bytes, relay_addr, timeout_s: float):
    """阻塞式传输循环，在后台线程中运行；结果写回 state。"""
    total = len(data)
    tile_uuid = uuid.UUID(state.tile_id)
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
        while idx < len(segments):
            off, ln, seq = segments[idx]
            if acked.covers(off, off + ln):
                # 迟到的 ACK 已覆盖该段，无需再发
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
                parsed = unpack_ack(datagram)
                if parsed is None:
                    continue
                ack_tid, ack_off, ack_len, _ack_seq = parsed
                if ack_tid != tile_uuid:
                    continue
                acked.add(ack_off, ack_off + ack_len)
                if acked.covers(off, off + ln):
                    idx += 1
                    consecutive_timeouts = 0
                    break
                # 否则是迟到/重复的 ACK，继续等当前段的确认
        state.mark(status="done")
    finally:
        sock.close()
