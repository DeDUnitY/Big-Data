from collections import defaultdict
from typing import List, Tuple


def mapper(points: List[Tuple[float, float]]):
    mapped = []
    for x, y in points:
        mapped.append(("stats", (x, y, x*x, x*y, 1)))
    return mapped


def shuffle_phase(mapped):
    grouped = defaultdict(list)
    for key, value in mapped:
        grouped[key].append(value)
    return grouped


def reducer(grouped):
    for values in grouped.values():
        sx = sy = sx2 = sxy = n = 0
        for x, y, x2, xy, cnt in values:
            sx += x
            sy += y
            sx2 += x2
            sxy += xy
            n += cnt

        a = (n * sxy - sx * sy) / (n * sx2 - sx ** 2)
        b = (sy - a * sx) / n
        return a, b


def linear_regression_mr(points):
    mapped = mapper(points)
    shuffled = shuffle_phase(mapped)
    return reducer(shuffled)


if __name__ == "__main__":
    data = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 11)]
    a, b = linear_regression_mr(data)
    print(f"y = {a:.2f}x + {b:.2f}")
