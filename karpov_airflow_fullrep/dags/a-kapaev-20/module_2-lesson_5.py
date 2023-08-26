"""
Lesson 5
"""
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_kapaev_20_plugins.a_kapaev_20_ram_locations import AKapaev20ramlocation


DEFAULT_ARGS = {
    'start_date': datetime (2023, 5, 15), # указываем дату старта ДАГа
    'end_date': datetime (2023, 5, 17), # указываем дату завершения работы ДАГа
    'owner': 'a-kapaev-20',
    'poke_interval': 600
}

dag = DAG("a-kapaev-20-module_2-lesson_5",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-kapaev-20']
          )

#предотвращение повторного создания таблицы
table_creation = PostgresOperator(
    task_id='table_creation',
    postgres_conn_id='conn_greenplum_write',
    sql='''
        CREATE TABLE IF NOT EXISTS a_kapaev_20_ram_location (
            id int NOT NULL,
	        name varchar(1024) NULL,
	        type varchar(1024) NULL,
	        dimension varchar(1024) NULL,
	        resident_cnt int NULL,
	        CONSTRAINT pkey PRIMARY KEY (id)
        )
        DISTRIBUTED BY (id);
        TRUNCATE TABLE a_kapaev_20_ram_location;
        ''',
    autocommit=True,
    dag=dag
)

top_locations = AKapaev20ramlocation(task_id='top_locations',
                                         num_of_locations=3,
                                         dag=dag)


table_creation >> top_locations