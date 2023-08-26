"""
Загрузка top_3 локаций
"""
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import BranchPythonOperator
from k_smirnov_plugins.k_smirnov_ram_location_operator import RAMLocationOperator
from k_smirnov_plugins.k_smirnov_fission_sensor import FissionSensor

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-smirnov',
    'poke_interval': 600
}

dag = DAG("k-smirnov_top3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['k-smirnov']
          )

geiger_counter = FissionSensor(
    task_id='geiger_counter',
    mode='reschedule',
    range_number=2,
    dag=dag
)


def check_table():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("check_table_cursor")
    cursor.execute("""SELECT EXISTS (SELECT 1
                           FROM information_schema.tables
                           WHERE table_name = 'k_smirnov_ram_location' 
                             AND table_schema = 'public') AS table_exists;
    """)
    return cursor.fetchone()[0]


check_table = PythonOperator(
    task_id='check_table',
    python_callable=check_table,
    dag=dag
)


def is_table_exist(table_exists):
    next_task = 'create_table'
    if table_exists == 'True':
        next_task = 'get_top3_location'
    return next_task


is_table_exist = BranchPythonOperator(
    task_id='is_table_exist',
    op_kwargs={"table_exists": "{{ti.xcom_pull('check_table')}}"},
    python_callable=is_table_exist,
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql="""
            CREATE TABLE public.k_smirnov_ram_location (
                id int NOT NULL,
                "name" varchar(200) NULL,
                "type" varchar(200) NULL,
                dimension varchar(200) NULL,
                resident_cnt int NULL,
                CONSTRAINT k_smirnov_ram_location_pkey PRIMARY KEY (id)
            )
            DISTRIBUTED BY (id);
          """,
    dag=dag
)

get_top3_location = RAMLocationOperator(
    task_id='get_top3_location',
    trigger_rule='one_success',
    dag=dag
)


def write_to_gp(top3_location):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    sql = """
            INSERT INTO public.k_smirnov_ram_location (id, name, type, dimension, resident_cnt) 
            SELECT id, name, type, dimension, resident_cnt
                    FROM (
                        VALUES 
                                {}
                        ) tmp(id, name, type, dimension, resident_cnt)
            WHERE NOT EXISTS (SELECT 1 FROM public.k_smirnov_ram_location WHERE id = tmp.id);
          """
    values = ""
    values_temp = "({}, '{}', '{}', '{}', {}),"
    top3_location_dic = eval(top3_location)
    for location in top3_location_dic:
        values += values_temp.format(location['id'], location['name'], location['type'], location['dimension'],
                                     location['residents'])
    sql = sql.format(values[:-1])
    pg_hook.run(sql)


write_to_gp = PythonOperator(
    task_id='write_to_gp',
    python_callable=write_to_gp,
    op_kwargs={"top3_location": "{{ti.xcom_pull('get_top3_location')}}"},
    dag=dag
)

geiger_counter >> check_table >> is_table_exist >> create_table  >> get_top3_location >>write_to_gp

