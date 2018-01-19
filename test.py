# -*- coding: utf-8 -*-

from dask.multiprocessing import get

from sizer import getsize


def test_1(*args):
    return args[0]


def test_2(*args):
    return args[0]


def _start():
    graph = {
        'a': 'a1',
        'b': 'a2',
        'par': ['a', 'b'],
        'data': (test, 'par'),
        'aa': 'data'[0],
        'bb': 'data'[1],
        'cc': 'data'[2],
        'res': (test1, 'aa', 'bb'),
    }
    res = get(graph, ['data', 'aa', 'bb', 'cc', 'res'])
    print(res)


if __name__ == '__main__':
    _start()
