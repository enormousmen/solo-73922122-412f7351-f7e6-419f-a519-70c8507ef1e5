"""IntervalSet 单元测试（无第三方依赖，直接 python 运行）。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.intervals import IntervalSet  # noqa: E402


def test_add_disjoint_and_merge():
    s = IntervalSet()
    assert s.add(0, 100) == 100
    assert s.add(200, 300) == 100
    assert s.total() == 200
    # 桥接两个区间，新增部分只有 (100,200)
    assert s.add(50, 250) == 100
    assert s.as_list() == [[0, 300]]
    assert s.total() == 300


def test_overlap_counted_once():
    s = IntervalSet()
    assert s.add(0, 1168) == 1168
    assert s.add(0, 1168) == 0          # 完全重复
    assert s.add(1000, 1500) == 332     # 部分重叠
    assert s.add(0, 1500) == 0          # 已被整体覆盖
    assert s.total() == 1500


def test_adjacent_merges():
    s = IntervalSet()
    s.add(0, 568)
    s.add(568, 1136)
    assert s.as_list() == [[0, 1136]]


def test_covers():
    s = IntervalSet()
    s.add(0, 100)
    s.add(200, 300)
    assert s.covers(0, 100)
    assert s.covers(10, 90)
    assert not s.covers(90, 210)
    assert not s.covers(150, 160)


def test_complement():
    s = IntervalSet()
    assert s.complement(100) == [(0, 100)]
    s.add(0, 30)
    s.add(50, 80)
    assert s.complement(100) == [(30, 50), (80, 100)]
    s.add(30, 50)
    s.add(80, 100)
    assert s.complement(100) == []
    # limit 截断
    s2 = IntervalSet()
    s2.add(90, 200)
    assert s2.complement(100) == [(0, 90)]


if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_"):
            fn()
            print(f"PASS {name}")
    print("all interval tests passed")
