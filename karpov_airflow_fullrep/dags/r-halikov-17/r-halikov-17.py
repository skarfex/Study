"""
task 04 - airflow

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

from airflow.sensors.time_delta import TimeDeltaSensor

from datetime import datetime, timedelta

# DEFAULT_ARGS = {
#     'owner': 'karpov',
#     'queue': 'karpov_queue',
#     'pool': 'user_pool',
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'depends_on_past': False,
#     'wait_for_downstream': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
#     'priority_weight': 10,
#     'start_date': datetime(2021, 1, 1),
#     'end_date': datetime(2025, 1, 1),
#     'sla': timedelta(hours=2),
#     'execution_timeout': timedelta(seconds=300),
#     'on_failure_callback': some_function,
#     'on_success_callback': some_other_function,
#     'on_retry_callback': another_function,
#     'sla_miss_callback': yet_another_function,
#     'trigger_rule':  'all_success'
# }

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'khalikov',
    'poke_interval': 600
}

with DAG(
        "khalikov_task_04",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['khalikov_task_04'],
) as dag:
    def check_sunday(**kwargs):
        execution_dt = kwargs["templates_dict"]["execution_dt"]
        logging.info(f"\t\t\texecution_dt == {execution_dt}")
        exec_day = datetime.strptime(execution_dt, "%Y-%m-%d").weekday()
        logging.info(f"\t\t\texec_day == {exec_day}")
        ans = "sunday"
        if exec_day != 6:
            ans = "not_sunday"

        return ans

    ch_sunday = BranchPythonOperator(
        task_id="check_sunday",
        python_callable=check_sunday,
        templates_dict={"execution_dt": "{{ ds }}"},
        dag=dag
    )

    def sunday_my():
        logging.info("\t\t\t ============ sunday ============")

    sunday = PythonOperator(
        task_id="sunday",
        python_callable=sunday_my,
        dag=dag
    )

    def read_data(**kwargs):
        # pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        # conn = pg_hook.get_conn()  # берём из него соединение
        # cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        # cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")

        execution_dt = kwargs["templates_dict"]["execution_dt"]
        logging.info(f"\t\t\texecution_dt == {execution_dt}")
        exec_day = datetime.strptime(execution_dt, "%Y-%m-%d").weekday()
        exec_day += 1
        logging.info(f"\t\t\texec_day == {exec_day}")
        query = (
            f"""\t\t\t
            select
                heading
            from articles
            where id = {exec_day}
            """
        )

        cursor.execute(query)
        # query_res = cursor.fetchall()
        # logging.info(f"\t\t\tquery_res == {query_res}")
        one_string = cursor.fetchone()[0]

        logging.info(one_string)

        return


    not_sunday = PythonOperator(
        task_id="not_sunday",
        templates_dict={"execution_dt": "{{ ds }}"},
        python_callable=read_data,
        dag=dag
    )

    ch_sunday >> [sunday, not_sunday]