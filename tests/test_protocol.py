"""协议编解码单元测试（无第三方依赖，直接 python 运行）。"""
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.protocol import (  # noqa: E402
    HDR_ACK,
    HDR_DATA,
    pack_ack,
    pack_data,
    unpack_ack,
    unpack_data,
)


def test_data_roundtrip():
    tid = uuid.uuid4()
    payload = b"x" * 568
    dg = pack_data(tid, 3504, 8192, 7, payload)
    assert len(dg) == HDR_DATA.size + 568
    tid2, off, total, seq, pl = unpack_data(dg)
    assert (tid2, off, total, seq, pl) == (tid, 3504, 8192, 7, payload)


def test_ack_roundtrip():
    tid = uuid.uuid4()
    dg = pack_ack(tid, 1168, 1168, 3)
    assert len(dg) == HDR_ACK.size == 32
    tid2, off, ln, seq = unpack_ack(dg)
    assert (tid2, off, ln, seq) == (tid, 1168, 1168, 3)


def test_reject_bad_packets():
    tid = uuid.uuid4()
    assert unpack_data(b"") is None
    assert unpack_data(b"DTL1") is None
    assert unpack_data(pack_data(tid, 0, 10, 0, b"")) is None          # 空载荷
    assert unpack_data(pack_data(tid, 8, 10, 0, b"abc")) is None       # 越界
    assert unpack_data(pack_ack(tid, 0, 1, 0)) is None                 # magic 不符
    assert unpack_ack(b"ACK1") is None
    assert unpack_ack(pack_data(tid, 0, 10, 0, b"abc")) is None        # 长度不符


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_"):
            fn()
            print(f"PASS {name}")
    print("all protocol tests passed")
