"""
Три локации сериала "Рик и Морти" fuuuu с наибольшим количеством резидентов
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_filatova_20_plugins.a_filatova_ram_location import AFilatova20ramlocatioper

DEFAULT_ARGS = {
    'start_date': datetime(2023, 5, 11),
    'end_date': datetime(2023, 5, 14),
    'owner': 'a-filatova-20',
    'poke_interval': 600
}

dag = DAG("a_filatova_20_ram_locations",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-filatova-20']
          )

#предотвращение повторного создания таблицы
table_creation = PostgresOperator(
    task_id='table_creation',
    postgres_conn_id='conn_greenplum_write',
    sql='''
        CREATE TABLE IF NOT EXISTS a_filatova_20_ram_location (
            id int NOT NULL,
	        name varchar(1024) NULL,
	        type varchar(1024) NULL,
	        dimension varchar(1024) NULL,
	        resident_cnt int NULL,
	        CONSTRAINT pkey PRIMARY KEY (id)
        )
        DISTRIBUTED BY (id);
        TRUNCATE TABLE a_filatova_20_ram_location;
        ''',
    autocommit=True,
    dag=dag
)

top_locations = AFilatova20ramlocatioper(task_id='top_locations',
                                         num_of_locations=3,
                                         dag=dag)


table_creation >> top_locations
