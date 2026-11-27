"""接收端：UDP 重组 + HTTP 查询/下载。

- UDP 收到数据段后按原字节位置写入缓冲区，重叠部分只计一次，并回 ACK；
- 当前缓冲区不重复字节数达到 total（整块收齐）时，额外回一份“收齐回执”
  （CPL1）。已收齐后再收到任何分片（含重复分片）也会重发收齐回执，
  这样发送端丢了一份收齐回执时，重试探测即可重新确认成功；
- GET /tiles/{id}/status   返回已收的不重复字节数、重复字节数、区间等；
- GET /tiles/{id}/download 仅当本进程重组完整时返回原图块。
"""
import os
import socket
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from .intervals import IntervalSet
from .protocol import MAX_DATAGRAM, pack_ack, pack_complete, unpack_data

UDP_PORT = int(os.environ.get("UDP_PORT", "9000"))

# ---- 仅用于演示 / 测试的故障注入 -------------------------------------------
# 收到 N 个有效分片后立即以非 0 退出码结束进程（先回 ACK，再“崩溃”），
# 由容器 restart 策略或测试脚本拉起新进程，从而模拟“接收进程中途重启、
# 内存中的已收分片全部丢失”。FAULT_MARKER 在进程重启后仍存在（容器可写层
# 或测试临时目录），保证一个场景只触发一次。
FAULT_EXIT_AFTER_DATAGRAMS = int(os.environ.get("FAULT_EXIT_AFTER_DATAGRAMS", "0"))
FAULT_MARKER = os.environ.get("FAULT_MARKER", "/tmp/tile-receiver-fault-fired")


def _fault_armed() -> bool:
    return FAULT_EXIT_AFTER_DATAGRAMS > 0 and not os.path.exists(FAULT_MARKER)


def _fault_fire():
    try:
        with open(FAULT_MARKER, "w", encoding="utf-8") as fh:
            fh.write("fired\n")
    except OSError:
        pass
    print(f"[fault] exiting after {FAULT_EXIT_AFTER_DATAGRAMS} datagrams "
          f"to simulate a receiver restart", flush=True)
    os._exit(3)
# ---------------------------------------------------------------------------


class TileBuffer:
    """单个图块的重组缓冲区。"""

    def __init__(self, total: int):
        self.total = total
        self.buf = bytearray(total)
        self.ranges = IntervalSet()
        self.datagrams = 0         # 收到的数据包总数（含重复）
        self.duplicate_bytes = 0   # 重叠部分累计字节数
        self.complete_acks = 0     # 本进程发出的收齐回执数（含重发）

    def add(self, offset: int, payload: bytes):
        self.buf[offset:offset + len(payload)] = payload
        added = self.ranges.add(offset, offset + len(payload))
        self.duplicate_bytes += len(payload) - added
        self.datagrams += 1

    @property
    def unique_bytes(self) -> int:
        return self.ranges.total()

    @property
    def complete(self) -> bool:
        return self.unique_bytes >= self.total


class Reassembly:
    def __init__(self):
        self._tiles: dict[str, TileBuffer] = {}
        self._lock = threading.Lock()

    def add_segment(self, tile_id: str, total: int, offset: int, payload: bytes):
        """写入一段并返回该图块缓冲区；total 不一致时返回 None。"""
        with self._lock:
            tile = self._tiles.get(tile_id)
            if tile is None:
                tile = self._tiles[tile_id] = TileBuffer(total)
            if tile.total != total:
                return None
            tile.add(offset, payload)
            return tile

    def get(self, tile_id: str):
        with self._lock:
            return self._tiles.get(tile_id)

    def record_complete_ack(self, tile: TileBuffer):
        with self._lock:
            tile.complete_acks += 1


reasm = Reassembly()


def _udp_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
    fault_remaining = FAULT_EXIT_AFTER_DATAGRAMS if _fault_armed() else 0
    while True:
        datagram, addr = sock.recvfrom(MAX_DATAGRAM)
        parsed = unpack_data(datagram)
        if parsed is None:
            continue
        tile_uuid, offset, total, seq, payload = parsed
        tile = reasm.add_segment(tile_uuid.hex, total, offset, payload)
        if tile is None:
            continue
        sock.sendto(pack_ack(tile_uuid, offset, len(payload), seq), addr)
        if tile.complete:
            # 只有当前接收进程整块收齐后才发收齐回执。
            # 已收齐时再次收到分片（发送端的探测/重传）也会重发，用于找回丢失的回执。
            reasm.record_complete_ack(tile)
            sock.sendto(pack_complete(tile_uuid, total, seq), addr)
        if fault_remaining > 0:
            # 先回分片确认再退出：模拟“回执已发出、进程随即重启丢内存”。
            fault_remaining -= 1
            if fault_remaining == 0:
                _fault_fire()

@asynccontextmanager
async def lifespan(_app):
    threading.Thread(target=_udp_loop, daemon=True).start()
    yield


app = FastAPI(title="Tile Receiver", lifespan=lifespan)


@app.get("/tiles/{tile_id}/status")
def tile_status(tile_id: str):
    tile = reasm.get(tile_id)
    if tile is None:
        raise HTTPException(404, "unknown tile")
    return {
        "tile_id": tile_id,
        "total": tile.total,
        "unique_bytes": tile.unique_bytes,
        "duplicate_bytes": tile.duplicate_bytes,
        "datagrams": tile.datagrams,
        "complete_acks": tile.complete_acks,
        "complete": tile.complete,
        "intervals": tile.ranges.as_list(),
    }


@app.get("/tiles/{tile_id}/download")
def tile_download(tile_id: str):
    tile = reasm.get(tile_id)
    if tile is None:
        raise HTTPException(404, "unknown tile")
    if not tile.complete:
        raise HTTPException(409, "tile incomplete")
    return Response(content=bytes(tile.buf), media_type="application/octet-stream")


@app.get("/health")
def health():
    return {"ok": True}
