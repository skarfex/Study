""" (03) Автоматизация ETL-процессов >> 4 урок >> Задания

Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:

Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью 
расписания или операторов ветвления)

Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри.

Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте 
его самостоятельно в вашем личном Airflow. Параметры соединения:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: karpovcourses
Login: student
Password: Wrhy96_09iPcreqAS

• Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds 
  (понедельник=1, вторник=2, ...)
• Выводить результат работы в любом виде: в логах либо в XCom'е

Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года

"""

from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

with DAG(
    dag_id='i-derkach-16-03-04',
    start_date = datetime(2022, 3, 1),
    end_date = datetime(2022, 3, 14),
    schedule_interval = '0 0 * * MON-SAT',
    max_active_runs = 10,
    catchup = True,
    tags=['i-derkach']
) as dag:

    def _get_heading(**kwargs) -> None:
        data = None
        try:
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f"""
                select heading
                from public.articles
                where id = {kwargs['next_execution_date'].isoweekday()}
                ;
            """)
            data = cursor.fetchall()
        except Exception as e:
            raise AirflowSkipException(e)
        finally:
            return data[0][0] if data else ''

    PythonOperator(
        task_id = 'get_heading',
        python_callable = _get_heading,
        provide_context = True
    )