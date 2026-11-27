"""按字节位置记录已确认/已接收区间的集合，重叠部分只计一次。"""


class IntervalSet:
    """维护一组有序、互不重叠的半开区间 [start, end)。"""

    def __init__(self):
        self._iv: list[list[int]] = []

    def add(self, start: int, end: int) -> int:
        """并入区间 [start, end)，返回新增的不重复字节数。"""
        if end <= start:
            return 0
        added = end - start
        keep: list[list[int]] = []
        ms, me = start, end
        for s, e in self._iv:
            overlap = max(0, min(me, e) - max(ms, s))
            if overlap > 0 or e == ms or me == s:
                # 相交或相邻：合并，并从新增计数中扣除重叠部分
                added -= overlap
                ms, me = min(ms, s), max(me, e)
            else:
                keep.append([s, e])
        keep.append([ms, me])
        keep.sort()
        self._iv = keep
        return added

    def total(self) -> int:
        """不重复字节总数。"""
        return sum(e - s for s, e in self._iv)

    def covers(self, start: int, end: int) -> bool:
        """[start, end) 是否已被某个区间完整覆盖。"""
        return any(s <= start and e >= end for s, e in self._iv)

    def complement(self, limit: int) -> list[tuple[int, int]]:
        """[0, limit) 内尚未覆盖的区间列表（升序）。"""
        gaps: list[tuple[int, int]] = []
        pos = 0
        for s, e in self._iv:
            if pos < min(s, limit):
                gaps.append((pos, min(s, limit)))
            pos = max(pos, e)
            if pos >= limit:
                break
        if pos < limit:
            gaps.append((pos, limit))
        return [(s, e) for s, e in gaps if e > s]

    def as_list(self) -> list[list[int]]:
        return [list(iv) for iv in self._iv]
