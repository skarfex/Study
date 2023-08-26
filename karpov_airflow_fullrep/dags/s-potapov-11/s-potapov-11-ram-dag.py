from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_potapov_11.s_potapov_11_ramapi_loc_operator import GetTopLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-potapov-11',
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1),
}


dag_params = {
    'dag_id': 's-potapov-11_rickandmorty_dag',
    'catchup': False,
    'default_args': DEFAULT_ARGS,
    'tags': ['s-potapov-11'],
    'schedule_interval': '@daily'
}

with DAG(**dag_params) as dag:

    start = DummyOperator(
        task_id='start',
    )


    end = DummyOperator(
        task_id='end',
    )


    ram_operator = GetTopLocationOperator(
        task_id='ram_operator',
    )


    create_gp_table = PostgresOperator(
        task_id='create_gp_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS s_potapov_11_ram_location
            (
                resident_cnt integer,
                id integer PRIMARY KEY,
                name varchar(512) not null,
                type varchar(256) not null,
                dimension varchar(512) not null
                
            )
            DISTRIBUTED BY (id);
        """,
        autocommit=True,
    )


    load_gp = PostgresOperator(
        task_id='load_gp',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE s_potapov_11_ram_location",
            "INSERT INTO s_potapov_11_ram_location VALUES {{ ti.xcom_pull(task_ids='ram_operator') }}",
        ],
        autocommit=True,
    )


    start >> ram_operator >> create_gp_table >> load_gp >> end


ram_operator.doc_md = """Оператор получения результата по API"""
create_gp_table.doc_md = """Создание таблицы в Greenplum"""
load_gp.doc_md = """Загрузка данных в Greenplum"""
