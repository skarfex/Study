"""
Даг находит 3 локации с наибольшим числом жителей.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from v_talnikov_12_plugins.v_talnikov_12_operator import TalnikovRaMOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'v_talnikov_12',
    'poke_interval': 600
}

with DAG("v_talnikov_12-ram",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=[' v_talnikov_12']
         ) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            create table if not exists public.v_talnikov_12_ram_location(
                id int, 
                name text, 
                type text, 
                dimension text, 
                resident_cnt int
            )
            DISTRIBUTED BY (id);
        """
    )

    find_most = TalnikovRaMOperator(
        task_id='find_most',
    )

    clean_table = PostgresOperator(
        task_id='clean_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            TRUNCATE TABLE public.v_talnikov_12_ram_location
        """
    )

    def update(**kwargs):
        task_instance = kwargs['ti']
        result = task_instance.xcom_pull(task_ids='find_most')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        for index, row in result.iterrows():
            sql_statement = f"""
                INSERT INTO public.v_talnikov_12_ram_location VALUES ('{row['id']}', 
                '{row['name']}', '{row['type']}', '{row['dimension']}', '{row['resident_cnt']}');
                """
            pg_hook.run(sql_statement, False)


    load_data = PythonOperator(
        task_id='load_data',
        python_callable=update
    )


create_table >> find_most >> clean_table >> load_data

dag.doc_md = __doc__