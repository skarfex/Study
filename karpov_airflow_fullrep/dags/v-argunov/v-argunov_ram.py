"""
First DAG v-argunov
"""
from airflow import DAG

import logging
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pendulum

from v_argunov_plugins.v_argunov_operator import VArgunovRamTopLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-argunov',
    'poke_interval': 600
}
with DAG("v-argunov_lesson5",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov']
) as dag:

    write_to_gp = VArgunovRamTopLocationOperator(
                    task_id='Write_top_locations'
                    )


    def check_empty_table_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT CASE WHEN EXISTS(SELECT 1 '
                       f'FROM public.v_argunov_ram_location) THEN 0 ELSE 1 END AS IsEmpty;')

        is_empty = cursor.fetchone()
        is_empty = int(str(is_empty)[1])

        if is_empty:
            logging.info('Table is empty, retrieving the values.')
            return 'Write_top_locations'
        else:
            logging.info('Table is not empty, aborting the procedure.')
            return 'Table_not_empty'

    table_not_empty = BashOperator(
        task_id='Table_not_empty',
        bash_command='echo Table not empty'
        )

    check_empty_table = BranchPythonOperator(
        task_id='check_empty_table',
        python_callable=check_empty_table_func,
    )


    check_empty_table >> [write_to_gp, table_not_empty]
