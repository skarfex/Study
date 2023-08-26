# -*- coding: utf-8 -*-
#
# l-fedjaev-13-ram-location.py
# Written by Leo Fedyaev, 2022-10-10
#
# Задание:
# 1. Создать в GreenPlum таблицу с названием l_fedjaev_13_ram_location
#    с полями (id, name, type, dimension, resident_cnt).
# 2. С помощью API (https://rickandmortyapi.com/documentation/#location) найти
#    три локации сериала «Рик и Морти» с наибольшим количеством резидентов.
# 3. Записать значения соответствующих полей этих трёх локаций в таблицу.


import logging
import pendulum as pdl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from l_fedjaev_13_plugins.InsertRamLocationsIntoTable import InsertRamLocationsIntoTable


@dag(
    dag_id='l-fedjaev-13-ram-location',

    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,

    tags=['leo', 'greenplum', 'ram', 'location'],

    # наследуются всеми задачами
    default_args={
        'start_date': pdl.today('UTC').add(days=-2),
        'owner': 'l-fedjaev-13', # airflow login
        'poke_interval': 600,
    },
)
def pipeline():
    #
    # Переменные
    #
    conn = 'conn_greenplum_write'
    table = 'l_fedjaev_13_ram_location'

    create = PostgresOperator(
        task_id='create',
        postgres_conn_id=conn,
        sql=f'''create table if not exists {table} (
                  id serial primary key,
                  name varchar(100) not null,
                  type varchar(100) not null,
                  dimension varchar(100) not null,
                  resident_cnt int4 not null
                ) distributed by (id);
        ''', autocommit=True,
    )

    clear = PostgresOperator(
        task_id='clear',
        postgres_conn_id=conn,
        sql=f'truncate {table};',
        autocommit=True,
    )

    insert = InsertRamLocationsIntoTable(
        task_id='insert',
        conn_id=conn,
        table_name=table,
        loc_limit=3,
    )

    check = PostgresOperator(
        task_id='check',
        postgres_conn_id=conn,
        sql=f'select * from {table};',
        autocommit=True,
    )


    #
    # Конвейер
    #
    create >> clear >> insert >> check


# Зарегистрировать себя
dag = pipeline()
