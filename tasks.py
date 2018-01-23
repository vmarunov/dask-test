# -*- coding: utf-8 -*-
import random
import time

from dask.distributed import Future


def futures_expand(func):

    def wrapper(*args, **kwargs):
        wrapper_args = []
        wrapper_kwargs = {}
        for arg in args:
            if isinstance(arg, Future):
                wrapper_args.append(arg.result())
            else:
                wrapper_args.append(arg)
        for key, arg in kwargs.items():
            if isinstance(arg, Future):
                wrapper_kwargs[key] = arg.result()
            else:
                wrapper_kwargs[key] = arg

        return func(*wrapper_args, **wrapper_kwargs)

    return wrapper


def _work(weight=0.01):
    u"""Имитация работы"""
    time.sleep(random.randint(1, 20) * weight)


@futures_expand
def load_data(isin):
    u"""
    Загрузка нвчальных данных из базы
    Возвращает param_a, param_b
    """
    _work()
    return {
        'param_a': '{}_param_a'.format(isin),
        'param_b': '{}_param_b'.format(isin)}


@futures_expand
def get_param(data, param):
    return data[param]


@futures_expand
def task_a(isin, param_a, param_b):
    u"""
    Одиночная задача. Рассчитывает независимый параметр для каждой облигации
    Можно выполнять независимо от любых других задач
    Зависит от загружаемых их базы param_a и param_b
    """
    _work()
    result = 'Task_A_result_{}_{}_{}'.format(isin, param_a, param_b)
    return result


@futures_expand
def grouper(*args):
    u"""
    Группирует входные параметры для групповой функции
    Возвращает в виде одного параметра
    """
    result = {}
    iterator = iter(args)
    try:
        while True:
            isin = next(iterator)
            task_a_res = next(iterator)
            param_b = next(iterator)
            result[isin] = {'task_a_res': task_a_res, 'param_b': param_b}
    except StopIteration:
        pass
    return result


@futures_expand
def task_group(group_data):
    u"""
    Групповая задача.
    На вход приходит словарь, {ISIN: данные}
    Взвращает некий групповой результат.
    Задача требует завершения task_a для всех облигаций
    """
    _work(0.1)
    result = {}
    for isin, data in group_data.iteritems():
        result[isin] = data
        result[isin]['group'] = random.randint(0, 100)
    return result


@futures_expand
def task_group_alter(*args):
    u"""
    Групповая задача.
    На вход поступают данные в порядке:
       isin1, param_a_isin1, param_b_isin_1, isin2, ....
    Взвращает некий групповой результат.
    Задача требует завершения task_a для всех облигаций
    """
    return task_group(grouper(*args))


@futures_expand
def task_b(isin, param_b, group_data):
    u"""
    Одиночная задача. Зависит от групповой задачи и от param_b
    """
    _work()
    result = 'Task_B_result_{}_{}_{}'.format(
        isin, param_b, group_data[isin]['group'])
    return result


@futures_expand
def task_c(isin, param_b):
    u"""
    Одиночная задача. Зависит от param_b
    """
    _work()
    result = 'Task_C_result_{}_{}'.format(isin, param_b)
    return result
