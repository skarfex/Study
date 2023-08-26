# """
#
# Тестовый даг из конспекта
#
# """
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from datetime import datetime,timedelta
# import logging
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.time_delta import TimeDeltaSensor
# from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import BranchPythonOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.edgemodifier import Label
#
# DEFAULT_ARGS = {
#     'start_date': days_ago(12), # дата, с которой генерируются DAG Run-ы
#     'owner': 'pirogov-p',
#     'poke_interval': 600 # задает интервал перезапуска сенсоров (каждые 600 с.)
# }
#
# with DAG(
#         dag_id='pirogov_p_dag2_conspect',  # уникальный идентификатор DAG-а внутри Airflow
#         schedule_interval='@daily',  # расписание
#         default_args=DEFAULT_ARGS,  # дефолтные переменные
#         max_active_runs=1,  # позволяет держать активным только один DAG Run
#         tags=['p-pirogov']  # тэги
# ) as dag:
#     # описываем таски и присваиваем их DAG-у
#     wait_until_6am = TimeDeltaSensor(
#         task_id='wait_until_6am',  # уникальный идентификатор таски внутри DAG
#         delta=timedelta(seconds=6 * 60 * 60)  # время, которое мы ждем от запуска DAG
#     )
#
#     echo_ds = BashOperator(
#         task_id='echo_ds',
#         bash_command='echo {{ ds }}' # выполняемый bash script (ds = execution_date)
#     )
#
#     def select_day_func(**kwargs):
#         execution_dt = kwargs['templates_dict']['execution_dt']
#         exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
#         return 'weekend' if exec_day in [5, 6] else 'weekday'
#
#
#     weekday_or_weekend = BranchPythonOperator(
#         task_id='weekday_or_weekend',
#         python_callable=select_day_func,
#         templates_dict={'execution_dt': '{{ ds }}'}
#
#     )
#
#     def weekday_func():
#         logging.info("It's weekday")
#
#
#     weekday = PythonOperator(
#         task_id='weekday',
#         python_callable=weekday_func # ссылка на функцию, выполняемую в рамках таски
#
#     )
#
#
#     def weekend_func():
#         logging.info("It's weekday")
#
#
#     weekend = PythonOperator(
#         task_id='weekend',
#         python_callable=weekend_func  # ссылка на функцию, выполняемую в рамках таски
#
#     )
#
#     eod = DummyOperator(
#         task_id='eod',
#         trigger_rule='one_success'
#
#     )
#
#     wait_until_6am >> echo_ds >> weekday_or_weekend >> [weekend, weekday] >> eod