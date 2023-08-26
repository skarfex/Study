'''
Урок №3-4 Рябушкин Максим Сергеевич
'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta,datetime
from airflow.hooks.postgres_hook import PostgresHook


default_args={
    'owner':'m-rjabushkin',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'start_date': datetime(2022,3,1), #4 lesson
    'end_date': datetime(2022,3,14), #4 lesson
    'schedule_interval':'00 01 * * 1-6' #4 lesson
}

def greet(name, age):
    print(f'Hello {name} and {age}')

#4 lesson
def query_postg(**kwargs):
    postgres = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = postgres.get_conn()
    ti=kwargs['ti']
    ex_dt=ti.xcom_pull(task_ids='Second_Task') #заберем дату запуска со bash таска
    ed = datetime.strptime(ex_dt, '%Y-%m-%d').weekday()+1 #определяем день недели, где 1=Пнд
    #logging.info(f'{ed}')
    with conn.cursor() as cur:
        cur.execute(f"SELECT heading FROM articles WHERE id = {ed}")
        res=cur.fetchall()
        return res


with DAG(
    'm-rjabushkin_lesson4',
    default_args=default_args,
    tags=['4lessons']
) as dag: 

    task1=TimeDeltaSensor(
        task_id='First_Task_Wait_Time',
        delta=timedelta(seconds=1)
    )

    task2=BashOperator(
        task_id='Second_Task',
        bash_command='echo {{ ds }}',
        trigger_rule='one_success')

    task3=PythonOperator(
        task_id='Hello',
        python_callable=greet,
        op_kwargs={'name':'Maksim','age':25}
    )
    
    task4=PythonOperator(
        task_id='query',
        python_callable=query_postg
    )

    task1>>task2>>task3>>task4
