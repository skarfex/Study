# -*- coding: utf-8 -*-
#
# l-fedjaev-13-acticle-heading.py
# Written by Leo Fedyaev, 2022-08-10
#
# Нужно создать DAG для Airflow, который должен:
# 1. Работать в период с 01.03.2022 по 14.03.2022, только в дни с понедельника по
#    субботу включительно (можно реализовать с помощью расписания или операторов
#    ветвления);
# 3. Работать с СУБД GreenPlum (karpov/c), через (PythonOperator и PostgresHook)
# 4. Забирать из таблицы =articles= значение поля =heading= из строки с =id=, равным дню
#    недели =ds= (понедельник=1, вторник=2, ...);
# 5. Выводить результат работы в журнал либо в XCom;


import logging
import pendulum as pdl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id='l-fedjaev-13-article-heading',
    # каждый месяц, каждый день, в 1:00 с понедельника по субботу включительно
    schedule_interval='0 1 * * 1-6',

    start_date=pdl.datetime(2022, 3, 1),
    end_date=pdl.datetime(2022, 3, 15),

    max_active_runs=1,
    tags=['leo', 'greenplum', 'article', 'heading'],

    # наследуются всеми задачами
    default_args=dict(
        owner='l-fedjaev-13', # airflow login
        poke_interval=600,
        catchup=False,
    ),
)
def pipeline():
    def fetch(id: int) -> str:
        """Получить заданное значение из БД"""

        # postgres_conn_id должен быть создан в Airflow
        hook = PostgresHook('conn_greenplum')
        cursor = hook.get_conn().cursor()

        cursor.execute(f'select heading from articles where id = {id}')
        return cursor.fetchall()

    #
    # Задачи (tasks)
    #
    @task
    def start(**kwargs):
        logging.info(f': {kwargs["ds"]}')

        return (pdl.from_format(kwargs['ds'], 'YYYY-MM-DD').weekday() + 1)


    
    @task(multiple_outputs=True)
    def extract(weekday: int) -> dict:
        return dict(id=weekday, heading=fetch(weekday))


    @task
    def store(data: dict) -> bool:
        logging.info(f'==> {data}')

        return True


    #
    # Конвейер (pipeline)
    #
    store(extract(start()))


# зарегистрировать себя
dag = pipeline()
