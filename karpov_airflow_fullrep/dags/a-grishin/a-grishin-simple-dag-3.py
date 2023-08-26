from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-grishin',
    'poke_interval': 600
}

with DAG("a-grishin-simple-dag3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-grishin']
         ) as dag:

    bash_echo_command = BashOperator(
        task_id='bash_echo_command',
        bash_command='echo -n "Bash command works fine"',
        dag=dag
    )

    def pyth_log_start():
        print('SUCCESS======DAG START=======')

    pyth_log_start = PythonOperator(
        task_id='pyth_log_start',
        python_callable=pyth_log_start,
        dag=dag
    )

    dummy_command = DummyOperator(
        task_id='dummy_command',
        dag=dag
    )

    def pyth_for_loop():
        print('=====FOR LOOP======: \n')
        for i in range(1, 6):
            print(i)

    pyth_for_loop = PythonOperator(
        task_id='pyth_for_loop',
        python_callable=pyth_for_loop,
        dag=dag
    )

    def pyth_log_end():
        print('SUCCESS====DAG END======')

    pyth_log_end = PythonOperator(
        task_id='pyth_log_end',
        python_callable=pyth_log_end,
        dag=dag
    )

    pyth_log_start >> [bash_echo_command, dummy_command] >> pyth_for_loop >> pyth_log_end
