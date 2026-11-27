"""UDP 分段传输协议：包格式与编解码。

数据包（发送端 -> 接收端）：
    magic(4) | tile_id(16) | offset(4) | total(4) | seq(4) | payload...
    头部 32 字节，UDP 载荷上限（1200/600）包含头部。

分片确认包（接收端 -> 发送端）：
    magic(4) | tile_id(16) | offset(4) | length(4) | seq(4)
    定长 32 字节，回执刚写入的字节区间 [offset, offset+length)。

收齐回执（接收端 -> 发送端）：
    magic(4) | tile_id(16) | total(4) | seq(4)
    定长 32 字节，由接收端在缓冲区不重复字节数达到 total 时发出（图块已完整
    存在于当前接收进程中时，收到重复分片也会再次发出）。

为什么需要单独的收齐回执：分片确认只证明“这一小段被某个接收进程写过”，
把重启前后两个接收进程各自回执的分片拼起来可以覆盖整块，但任何一个进程都
不持有完整图块。发送端只有收到收齐回执才能认定传输成功。
"""
import struct
import uuid

MAGIC_DATA = b"DTL1"
MAGIC_ACK = b"ACK1"
MAGIC_COMPLETE = b"CPL1"

HDR_DATA = struct.Struct("!4s16sIII")      # magic, tile_id, offset, total, seq
HDR_ACK = struct.Struct("!4s16sIII")       # magic, tile_id, offset, length, seq
HDR_COMPLETE = struct.Struct("!4s16sIII")  # magic, tile_id, total, seq, 保留(0)

MAX_DATAGRAM = 65535


def pack_data(tile_id: uuid.UUID, offset: int, total: int, seq: int, payload: bytes) -> bytes:
    return HDR_DATA.pack(MAGIC_DATA, tile_id.bytes, offset, total, seq) + payload


def unpack_data(datagram: bytes):
    """解析数据包，返回 (tile_id, offset, total, seq, payload)；非法包返回 None。"""
    if len(datagram) < HDR_DATA.size or len(datagram) > MAX_DATAGRAM:
        return None
    magic, tid, offset, total, seq = HDR_DATA.unpack(datagram[:HDR_DATA.size])
    if magic != MAGIC_DATA:
        return None
    payload = datagram[HDR_DATA.size:]
    if not payload or total == 0 or offset + len(payload) > total:
        return None
    return uuid.UUID(bytes=tid), offset, total, seq, payload


def pack_ack(tile_id: uuid.UUID, offset: int, length: int, seq: int) -> bytes:
    return HDR_ACK.pack(MAGIC_ACK, tile_id.bytes, offset, length, seq)


def unpack_ack(datagram: bytes):
    """解析分片确认包，返回 (tile_id, offset, length, seq)；非法包返回 None。"""
    if len(datagram) != HDR_ACK.size:
        return None
    magic, tid, offset, length, seq = HDR_ACK.unpack(datagram)
    if magic != MAGIC_ACK or length == 0:
        return None
    return uuid.UUID(bytes=tid), offset, length, seq


def pack_complete(tile_id: uuid.UUID, total: int, seq: int) -> bytes:
    return HDR_COMPLETE.pack(MAGIC_COMPLETE, tile_id.bytes, total, seq, 0)


def unpack_complete(datagram: bytes):
    """解析收齐回执，返回 (tile_id, total, seq)；非法包返回 None。"""
    if len(datagram) != HDR_COMPLETE.size:
        return None
    magic, tid, total, seq, _reserved = HDR_COMPLETE.unpack(datagram)
    if magic != MAGIC_COMPLETE or total == 0:
        return None
    return uuid.UUID(bytes=tid), total, seq
