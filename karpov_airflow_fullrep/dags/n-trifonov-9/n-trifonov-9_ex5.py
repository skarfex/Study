from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import ShortCircuitOperator

from airflow.hooks.postgres_hook import PostgresHook
from n_trifonov_9_plugins.nikita_resident_count_operator import NikitaResidentsCountOperator

DEFAULT_ARGS = {"start_date": datetime(2022, 3, 1), "end_date": datetime(2022, 10, 14), "owner": "n-trifonov-9"}

dag = DAG("n-trifonov-9-exercise5", schedule_interval=None, default_args=DEFAULT_ARGS, tags=["n-trifonov-9"])

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="conn_greenplum_write",
    sql="""
            CREATE TABLE IF NOT EXISTS ntrifonov9_ram_location (
            id VARCHAR PRIMARY KEY,
            name VARCHAR,
            type VARCHAR,
            dimension VARCHAR,
            resident_cnt INT);
          """,
    dag=dag,
)


def check_data_exists_func(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f"SELECT count(*) FROM ntrifonov9_ram_location")  # исполняем sql
    row_cnt = cursor.fetchall()[0][0]
    logging.info("rows: %d" % row_cnt)
    return row_cnt == 0  # полный результат


check_data_exists = ShortCircuitOperator(task_id="check_data_exists", python_callable=check_data_exists_func, dag=dag)

get_locations = NikitaResidentsCountOperator(task_id="get_locations", dag=dag)


def load_csv_to_gp_func():
    pg_hook = PostgresHook("conn_greenplum_write")
    pg_hook.copy_expert("COPY ntrifonov9_ram_location FROM STDIN DELIMITER ','", "/tmp/locations.csv")


load_csv_to_gp = PythonOperator(task_id="load_csv_to_gp", python_callable=load_csv_to_gp_func, dag=dag)


create_table >> check_data_exists >> get_locations >> load_csv_to_gp
