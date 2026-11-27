"""接收端：UDP 重组 + HTTP 查询/下载。

- UDP 收到数据段后按原字节位置写入缓冲区，重叠部分只计一次，并回 ACK；
- GET /tiles/{id}/status   返回已收的不重复字节数、重复字节数、区间等；
- GET /tiles/{id}/download 仅当重组完整时返回原图块。
"""
import os
import socket
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from .intervals import IntervalSet
from .protocol import MAX_DATAGRAM, pack_ack, unpack_data

UDP_PORT = int(os.environ.get("UDP_PORT", "9000"))


class TileBuffer:
    """单个图块的重组缓冲区。"""

    def __init__(self, total: int):
        self.total = total
        self.buf = bytearray(total)
        self.ranges = IntervalSet()
        self.datagrams = 0         # 收到的数据包总数（含重复）
        self.duplicate_bytes = 0   # 重叠部分累计字节数

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


reasm = Reassembly()


def _udp_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))
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
