'''
Практическое задание по уроку 5.
DAG создает таблицу "dmi-matveev_ram_location" в Greenplum
и загружает в нее данные по 3-м самым населенным локациям из вселенной сериала Rick&Morty
'''

from dmi_matveev_plugins.GetTocLocRickMortyOperator import GetTocLocRickMortyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
import logging

DEFAULT_ARGS = {
    'owner': 'matveevdm',
    'start_date': datetime(2023, 1, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        dag_id='dmi-matveev_lesson5_dag',
        schedule_interval=None,
        default_args=DEFAULT_ARGS,
        tags=['matveevdm'],
        catchup=False
) as dag:

    create_table_if_not_exists = PostgresOperator(task_id='create_table_if_not_exists',
                                                  postgres_conn_id='conn_greenplum_write',
                                                  sql='''CREATE TABLE IF NOT EXISTS "dmi-matveev_ram_location" (
                                                      id INTEGER PRIMARY KEY,
                                                      name VARCHAR,
                                                      type VARCHAR,
                                                      dimension VARCHAR,
                                                      resident_cnt INTEGER);'''
                                                  )

    get_top_loc_rick_morty = GetTocLocRickMortyOperator(task_id='get_top_loc_rick_morty',
                                                        top_loc=3)


    # Дополнить запрос обработкой конфликтов, для предотвращения дублей и ошибок уникальности
    def load_loc_to_greenplum(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        logging.info('Truncate target table')
        truncate_sql = '''TRUNCATE TABLE "dmi-matveev_ram_location";'''
        pg_hook.run(truncate_sql)
        top_locations = kwargs['ti'].xcom_pull(task_ids='get_top_loc_rick_morty')
        load_sql = '''INSERT INTO "dmi-matveev_ram_location" VALUES (
            %(id)s,
            %(name)s,
            %(type)s,
            %(dimension)s,
            %(resident_cnt)s
            );'''
        for x in top_locations:
            logging.info(f"Uploading the location to Greenplum - {x['name']}")
            pg_hook.run(load_sql, parameters={"id": x["id"],
                                         "name": x["name"],
                                         "type": x["type"],
                                         "dimension": x["dimension"],
                                         "resident_cnt": x['residents']
                                         }
                        )


    load_locations = PythonOperator(task_id='load_locations',
                                    python_callable=load_loc_to_greenplum,
                                    provide_context=True)

    create_table_if_not_exists >> get_top_loc_rick_morty >> load_locations

    dag.doc_md = __doc__
    create_table_if_not_exists.doc_md = '''Таск создает таблицу "dmi-matveev_ram_location" в Greenplum,
    если ее не существует'''
    get_top_loc_rick_morty.doc_md = '''Таск запрашивает из API список локаций и вычисляет 3 самые населенные.
    Результат возвращает в XCom'''
    load_locations.doc_md = '''Таск загружает результат работы задачи "get_top_loc_rick_morty" в Greenplum,
    предварительно зачищая таблицу "dmi-matveev_ram_location" от результатов работы прошлого дага'''
