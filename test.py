# -*- coding: utf-8 -*-
import random
import time

from dask.distributed import Client

from sizer import getsize


ISIN_COUNT = 50


def _work(weight=0.01):
    u"""Имитация работы"""
    time.sleep(random.randint(1, 20) * weight)


def get_isins():
    u"""
    Зачитать из базы список ISIN
    """
    return ['RU{}'.format(str(i).rjust(10, '0')) for i in range(ISIN_COUNT)]


def test_0(*args):
    return args[0]


def test_1(*args):
    return args[0]


def test_2(*args):
    return args[0]


def test_3(*args):
    return args[0]


def test_4(*args):
    return args[0]


def test_5(*args):
    return args[0]


def test_6(*args):
    return args[0]


def test_7(*args):
    return args[0]


def test_8(*args):
    return args[0]


def test_9(*args):
    return args[0]


def test_10(*args):
    return args[0]


def test_11(*args):
    return args[0]


def test_12(*args):
    return args[0]


def test_13(*args):
    return args[0]


def test_14(*args):
    return args[0]


def test_15(*args):
    return args[0]


def test_16(*args):
    return args[0]


def test_17(*args):
    return args[0]


def test_18(*args):
    return args[0]


def test_19(*args):
    return args[0]


def test_20(*args):
    return args[0]


def test_21(*args):
    return args[0]


def test_22(*args):
    return args[0]


def test_23(*args):
    return args[0]


def test_24(*args):
    return args[0]


def test_25(*args):
    return args[0]


def test_26(*args):
    return args[0]


def test_27(*args):
    return args[0]


def test_28(*args):
    return args[0]


def test_29(*args):
    return args[0]


def test_30(*args):
    return args[0]


def test_31(*args):
    return args[0]


def test_32(*args):
    return args[0]


def test_33(*args):
    return args[0]


def _start_get_size():
    graph = {}
    isins = get_isins()

    for c, i in enumerate(isins):
        params = tuple()
        for j in range(10):
            param = 'param_{}_{}'.format(c, j)
            graph[param] = '{} == {} == {}'.format(
                i, c, j)
            params += (param,)
        for j in range(34):
            graph['res_{}_{}'.format(c, j)] = (
                    (globals()['test_{}'.format(j)],) + params)
    print(len(graph))
    print(getsize(graph))
    c = Client('127.0.0.1:8786')
    result = zip(graph.keys(), c.get(graph, graph.keys()))
    print('OK')
    print(result)


#####################################################
# from dask import delayed


def load_data(isin):
    _work(0.03)
    return {
        'isin': isin,
        'data': 'data_for_{}'.format(isin)
    }


def _start_combine():
    import dask.bag as db
    c = Client('127.0.0.1:8786')
    isins = get_isins()
    data = c.map(load_data, isins)

    print(data[0].result())


if __name__ == '__main__':
    # _start_get_size()
    _start_combine()