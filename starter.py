# -*- coding: utf-8 -*-
from itertools import chain
import json
from time import time

from dask.distributed import Client

from tasks import (
    load_data, get_param, task_a, grouper, task_group, task_group_alter,
    task_b, task_c)

ISIN_COUNT = 500


def get_isins():
    u"""
    Зачитать из базы список ISIN
    """
    return ['RU{}'.format(str(i).rjust(10, '0')) for i in range(ISIN_COUNT)]


def start():
    t = time()
    isins = get_isins()

    # граф задач (состоит из списка ISIN)
    graph = {'isin_{}'.format(i): isin for i, isin in enumerate(isins)}

    # добавляем в граф задачи загрузки данных из БД
    # разбиения загруженных данных на параметры
    for i, isin in enumerate(isins):
        graph['data_{}'.format(i)] = (load_data, 'isin_{}'.format(i))
        graph['param_a_{}'.format(i)] = (
            get_param, 'data_{}'.format(i), 'param_a')
        graph['param_b_{}'.format(i)] = (
            get_param, 'data_{}'.format(i), 'param_b')

    # добавляем в граф задачу task_a
    for i, isin in enumerate(isins):
        graph['task_a_res_{}'.format(i)] = (
            task_a,
            'isin_{}'.format(i),
            'param_a_{}'.format(i),
            'param_b_{}'.format(i))

    # Добавляем в граф параметр для групповой задачи:
    param_list = []
    for i in range(len(isins)):
        param_list.append('isin_{}'.format(i))
        param_list.append('task_a_res_{}'.format(i))
        param_list.append('param_b_{}'.format(i))
    graph['group_data'] = (grouper,) + tuple(param_list)

    # Добавляем в граф групповую задачу
    graph['group_res'] = (task_group, 'group_data')

    # добавляем в граф задачу task_b
    for i, isin in enumerate(isins):
        graph['task_b_res_{}'.format(i)] = (
            task_b,
            'isin_{}'.format(i),
            'param_b_{}'.format(i),
            'group_res')

    # добавляем в граф задачу task_c
    for i, isin in enumerate(isins):
        graph['task_c_res_{}'.format(i)] = (
            task_c,
            'isin_{}'.format(i),
            'param_b_{}'.format(i))

    # Формирование списка получаемых результатов
    get_list = ['group_data']
    for i, isin in enumerate(isins):
        get_list.append('task_a_res_{}'.format(i))
        get_list.append('task_b_res_{}'.format(i))
        get_list.append('task_c_res_{}'.format(i))

    # Создаем client
    client = Client('127.0.0.1:8786')

    # Получение результатов
    result = client.get(graph, get_list)

    total = time() - t
    print(total)
    print(len(result))
    with open('/Users/vladimirmarunov/git/dask-test/res1.txt', 'w') as f:
        f.write('{}\n'.format(total))
        json.dump(result, f, indent=4)


def start_futures():
    t = time()
    isins = get_isins()

    client = Client('127.0.0.1:8786')

    data = client.map(load_data, isins)
    params_a = client.map(get_param, data, ['param_a'] * len(isins))
    params_b = client.map(get_param, data, ['param_a'] * len(isins))

    result_a = client.map(task_a, isins, params_a, params_b)

    group_args = list(chain(*zip(isins, result_a, params_b)))
    result_group = client.submit(task_group_alter, *group_args)

    result_b = client.map(task_b, isins, params_b, [result_group] * len(isins))

    result_c = client.map(task_c, isins, params_b)

    result = client.gather([result_group] + result_a + result_b + result_c)

    total = time() - t
    print(total)
    print(len(result))
    with open('/Users/vladimirmarunov/git/dask-test/res.txt', 'w') as f:
        f.write('{}\n'.format(total))
        json.dump(result, f, indent=4)


if __name__ == '__main__':
    start()
    start_futures()
