#!/bin/bash

# Скрипт запуска Jupyter Notebook

cd /opt

# Сгенерировать токен или использовать без токена
# Вариант 1: Без токена (небезопасно, но удобно для разработки)
jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token='' \
    --NotebookApp.password=''

# Вариант 2: С токеном 'hadoop' (альтернатива)
# jupyter notebook \
#     --ip=0.0.0.0 \
#     --port=8888 \
#     --no-browser \
#     --allow-root \
#     --NotebookApp.token='hadoop'


