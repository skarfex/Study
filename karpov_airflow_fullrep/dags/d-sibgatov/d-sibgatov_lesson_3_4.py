"""
Даг 3-4 урока
"""
from datetime import datetime

from airflow import DAG
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date':datetime(2022,3,1),
    'end_date':datetime(2022,3,14),
    'owner':'d-sibgatov'
}


with DAG("d-sibgatov_lesson_3_4",
         schedule_interval='0 4 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-sibgatov']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ts = BashOperator(
        task_id="echo_ts",
        bash_command="echo {{ ts }}",
        dag=dag
    )

    def get_date_execution_func(**kwargs):
        exec_date = kwargs["templates_dict"]["execute_date"]
        exec_date = datetime.strptime(exec_date, '%Y-%m-%d').weekday()
        logging.info(f'День недели: {exec_date}')
        kwargs['ti'].xcom_push(value=exec_date, key='execute_date')

    gate_date_execution = PythonOperator(
        task_id='gate_date_execution',
        python_callable=get_date_execution_func,
        templates_dict={'execute_date':'{{ ds }}'},
        provide_context=True,
        dag=dag
    )

    def day_week_func(**kwargs):
        exec_date = kwargs["ti"].xcom_pull(task_ids="gate_date_execution", key="execute_date")
        week = {0: 'Понедельник', 1: 'Вторник', 2: 'Среда', 3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'}
        logging.info("День недели: " +week[exec_date])


    day_week = PythonOperator(
        task_id="day_week",
        python_callable=day_week_func,
        provide_context=True,
        dag=dag
    )

    def get_data_gp_func(**kwargs):
        exec_date = kwargs["ti"].xcom_pull(task_ids="gate_date_execution", key="execute_date")
        exec_date = str(int(exec_date)+1)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id={0}'.format(exec_date))
        one_string = cursor.fetchone()[0]
        kwargs["ti"].xcom_push(value=one_string, key="result")


    get_data_gp = PythonOperator(
        task_id="get_data_gp",
        python_callable=get_data_gp_func,
        provide_context=True,
        dag=dag
    )


dummy >> [echo_ts, gate_date_execution]
gate_date_execution >> [day_week, get_data_gp]

dummy.doc_md = "dummy таск"
echo_ts.doc_md = "bash-оператор выводящий дату в формате ts в логи"
gate_date_execution.doc_md = "python-оператор выводящий дату операции в xcom"
day_week.doc_md = "python-оператор выводящий день недели в логи"
get_data_gp.doc_md = "python-оператор выводящий данные из таблицы articles"