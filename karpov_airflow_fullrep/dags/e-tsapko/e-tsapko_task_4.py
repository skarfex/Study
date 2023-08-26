"""
Даг забирает из таблицы Articles в Greenplum значения поля heading с id, равным дню недели.
Даг работает с 1 марта 2022 года до 14 марта 2022 года ежедневно, кроме воскресенья.
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-tsapko'
}

with DAG("e-tsapko_task_4",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-tsapko']
) as dag:

    def is_not_sunday_func(**kwargs) -> int:
        """Returns true if any week day but Sunday
        and pushes number of the weekday to XCom"""
        exec_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()
        kwargs['ti'].xcom_push(value=exec_day, key='day_of_week')
        return exec_day != 7

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=is_not_sunday_func,
        provide_context=True
    )

    def extract_heading_from_gp_func(**kwargs) -> None:
        """Connects to Greenplum
        and extracts the heading
        from the table 'articles'
        with id corresponding to the week day number"""

        day_of_week = kwargs['ti'].xcom_pull(task_ids='not_sunday', key='day_of_week')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
        logging.info(cursor.fetchall())  # полный результат

    extract_heading_from_gp = PythonOperator(
        task_id='extract_heading_from_gp',
        python_callable=extract_heading_from_gp_func,
        provide_context=True
    )

    not_sunday >> extract_heading_from_gp
