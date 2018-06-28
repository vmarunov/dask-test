#!/bin/sh

# Запуск шедулера
dask-scheduler

# Запуск воркеров
PYTHONPATH=. dask-worker 127.0.0.1:8786 &
PYTHONPATH=. dask-worker 127.0.0.1:8786 &
PYTHONPATH=. dask-worker 127.0.0.1:8786 &
