# -*- coding: utf-8 -*-
"""
Calculate size of object
"""


def _sort_size(sizes):
    els = sorted(sizes.keys())
    return tuple(enumerate(els)), [sizes[el] for el in els]


SET_SIZES = _sort_size({
    0: 232,
    6: 744,
    22: 2280,
    86: 8424,
    342: 33000,
    1366: 131304,
    5462: 524520,
    21846: 2097384,
    87382: 4194536,
    174763: 8388840,
    349526: 16777448,
    699051: 33554664})


DICT_SIZES = _sort_size({
    0: 280,
    6: 1048,
    22: 3352,
    86: 12568,
    342: 49432,
    1366: 196888,
    5462: 786712,
    21846: 3146008,
    87382: 6291736,
    174763: 12583192,
    349526: 25166104,
    699051: 50331928})


def _sizeof(obj):
    def _sizel(count, sizes):
        keys = [i for i in sizes[0] if i[1] <= count]
        return sizes[1][keys[-1][0]]

    if obj is None:
        return 16
    if isinstance(obj, unicode):
        return 52 + (len(obj) * 4)
    if isinstance(obj, str):
        return 37 + len(obj)
    if isinstance(obj, long):
        return 28
    if isinstance(obj, (int, float)):
        return 24
    if isinstance(obj, tuple):
        return 56 + len(obj) * 8
    if isinstance(obj, list):
        result = 72
        if obj:
            result += (24 + (len(obj) * 8 ))
        return result
    if isinstance(obj, set):
        return _sizel(len(obj), SET_SIZES)
    if isinstance(obj, dict):
        return _sizel(len(obj), DICT_SIZES)
    if isinstance(obj, object):
        return 64
    raise ValueError('UNKNOWN TYPE {}'.format(type(obj)))


def getsize(obj_0):
    """Recursively iterate to sum size of object & members."""
    def inner(obj, _seen_ids=set()):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = _sizeof(obj)
        if isinstance(obj, (basestring, xrange, bytearray)):
            pass  # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, set)):
            size += sum(inner(i) for i in obj)
        elif hasattr(obj, 'iteritems'):
            size += sum(inner(k) + inner(v) for k, v in obj.iteritems())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
            size += sum(
                inner(getattr(obj, s))
                for s in obj.__slots__ if hasattr(obj, s))
        return size
    return inner(obj_0)


if __name__ == '__main__':
    print(_sizeof(1.1))
