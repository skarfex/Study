from airflow import DAG
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'r-romanov',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)

}

dag = DAG("rm_load_from_gp",
          schedule_interval = '0 0 * * 1-6',
          default_args = DEFAULT_ARGS,
          max_active_runs = 1,
          tags = ['rm']
          )

# dummy = DummyOperator(
#     task_id = 'eod',
#     trigger_rule = 'one_success',
#     dag = dag
# )
#
# echo_ds = BashOperator(
#     task_id='echo_ds',
#     bash_command='echo {{ ds }}', # выполняемый bash script (ds = execution_date)
#     dag=dag
# )

# dummy = DummyOperator(task_id="dummy")
def go_to_gp(date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    date = (datetime.strptime(date,'%Y-%m-%d') + timedelta(days=1)).weekday()
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    logging.info(str(date))
    cursor.execute(f"SELECT heading FROM articles WHERE id = {date}")  # исполняем sql
    # query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(one_string)
    cursor.close()
    conn.close()


load_from_gp = PythonOperator(
    task_id='load_from_gp',
    python_callable=go_to_gp, # ссылка на функцию, выполняемую в рамках таски
    op_kwargs = {"date":'{{ds}}'},
    dag=dag
)

load_from_gp
