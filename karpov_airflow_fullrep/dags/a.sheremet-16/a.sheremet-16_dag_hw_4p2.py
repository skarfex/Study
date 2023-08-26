# == Урок 4. Задание 2 ==============================================
# DAG должен:
# 1. Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
# 2. Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
#    Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо 
#    настройте его самостоятельно в вашем личном Airflow. 
# 3. Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
# 4. Выводить результат работы в любом виде: в логах либо в XCom'е

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator, get_current_context
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'a.sheremet-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 19),
    'schedule_interval': '0 9 * 1 1-6'
}

dag = DAG('sam_dag_hw_4p2'
          , catchup=False
          , default_args=default_args
          , tags=['sheremet_am'])
    
start = DummyOperator(task_id = 'start', dag=dag)
end = DummyOperator(task_id = 'end', dag=dag)

def _get_article_func():
    pg_hook = PostgresHook('conn_greenplum')
    context = get_current_context()
    exec_date = context['ds']
    q = \
    f"""
    select id, heading 
    from public.articles 
    where id = extract(dow from '{exec_date}'::date);
    """
    df = pg_hook.get_pandas_df(sql=q)
    result = df.to_json(orient='records')
    return print(f"Result at {exec_date}\n The day of the week and the article heading\n {result}")
    
sam_get_article_task = PythonOperator(
    task_id='sam_get_article_task'
    , python_callable=_get_article_func
    , dag=dag)

start >> sam_get_article_task >> end