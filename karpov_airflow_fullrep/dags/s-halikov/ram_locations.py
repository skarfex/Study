from airflow import DAG
# from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from s_halikov_plugins.ram_loc import S_halikovRamLocationCountOperator
import datetime as dt
import logging

DEFAULT_ARGUMENTS = {
    'owner': 's-halikov'
}

with DAG(
    dag_id='s_halikov_ram_dag',
    default_args=DEFAULT_ARGUMENTS,
    tags=['salavat', 'ram'],
    schedule_interval='@once',
    start_date=dt.datetime.now()
) as s_halikov_ram_dag:
    TARGET_TABLE_NAME = "s_halikov_ram_location"

    def create_target_table_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        query = f"""
        DROP TABLE IF EXISTS public.{TARGET_TABLE_NAME};
        CREATE TABLE public.{TARGET_TABLE_NAME} (id text, name text, type text, dimension text, resident_cnt text);
        """
        pg_hook.run(query, False)

    create_target_table = PythonOperator(
        task_id='create_target_table_operator',
        python_callable=create_target_table_func
    )

    top_locations_operator = S_halikovRamLocationCountOperator(
        task_id='get_top_locations',
        trigger_rule='dummy')

    def write_operator_func(**kwargs):
        top_locations = kwargs['task_instance'].xcom_pull(task_ids='get_top_locations', key='top_locations')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        logging.info('len of top_locations: ' + str(len(top_locations)))
        values = []
        for location in top_locations:
            values.append(f'("{location[0]}", "{location[1]}", "{location[2]}", "{location[3]}", "{location[4]}")')
        query = f"INSERT INTO {TARGET_TABLE_NAME} VALUES " + ', '.join(values)
        logging.info("-------------------------------")
        logging.info(f"query:\n{query}")
        logging.info("-------------------------------")
        pg_hook.run(query, False)

    write_operator = PythonOperator(
        task_id='write_operator',
        python_callable=write_operator_func,
        provide_context=True
    )

    create_target_table >> top_locations_operator >> write_operator
