# -*- coding: utf-8 -*-
import random


def load_data(isin):
    u"""
    Загрузка нвчальных данных из базы
    Возвращает param_a, param_b
    """
    return {
        'param_a': '{}_param_a'.format(isin),
        'param_b': '{}_param_b'.format(isin)}


def get_param(data, param):
    return data[param]


def task_a(isin, param_a, param_b):
    u"""
    Одиночная задача. Рассчитывает независимый параметр для каждой облигации
    Можно выполнять независимо от любых других задач
    Зависит от загружаемых их базы param_a и param_b
    """
    result = 'Task_A_result_{}_{}_{}'.format(isin, param_a, param_b)
    return result


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
            param_a = next(iterator)
            param_b = next(iterator)
            result[isin] = {'param_a': param_a, 'param_b': param_b}
    except StopIteration:
        pass
    return result


def task_group(group_data):
    u"""
    Групповая задача.
    На вход приходит словарь, {ISIN: данные}
    Взвращает некий групповой результат.
    Задача требует завершения task_a для всех облигаций
    """
    result = {}
    for isin, data in group_data.iteritems():
        result[isin] = data
        result[isin]['group'] = random.randint(0, 100)
    return result


def task_b(isin, param_b, group_data):
    u"""
    Одиночная задача. Зависит от групповой задачи и от param_b
    """
    result = 'Task_B_result_{}_{}_{}'.format(
        isin, param_b, group_data[isin]['group'])
    return result
