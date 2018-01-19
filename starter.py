# -*- coding: utf-8 -*-
from dask.distributed import Client

from tasks import load_data, get_param, task_a, grouper, task_group, task_b

ISIN_COUNT = 500


def get_isins():
    u"""
    Зачитать из базы список ISIN
    """
    return ['RU{}'.format(str(i).rjust(10, '0')) for i in range(ISIN_COUNT)]


def start():

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
        param_list.append('param_a_{}'.format(i))
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

    # Формирование списка получаемых результатов
    get_list = []
    for i, isin in enumerate(isins):
        get_list.append('isin_{}'.format(i))
        get_list.append('task_a_res_{}'.format(i))
        get_list.append('task_b_res_{}'.format(i))

    # Создаем client
    client = Client('127.0.0.1:8786')

    # Получение результатов
    result = client.get(graph, get_list)
    print(len(result))
    with open('/Users/vladimirmarunov/git/dask-test/res.txt', 'w') as f:
        f.write(str(result))


if __name__ == '__main__':
    start()
