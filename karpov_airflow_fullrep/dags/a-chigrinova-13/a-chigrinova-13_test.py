"""
Тестовый даг a-chigrinova-13
Использует операторы Python, Bash, Dummy
1) Выводит строку "Bash command works fine"
2) Выводит строку "Python command works fine"
3) Прогоняет Dummy таску
4) Выписывает числа от 1 до 10
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-chigrinova-13',
    'poke_interval': 600
}

with DAG("a-chigrinova-13_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-chigrinova-13']
         ) as dag:

    bash_echo_command = BashOperator(
        task_id='bash_echo_command',
        bash_command='echo -n "Bash command works fine"',
        dag=dag
    )

    def python_print():
        print('Python command works fine')

    python_print_command = PythonOperator(
        task_id='python_print_command',
        python_callable=python_print,
        dag=dag
    )

    dummy_command = DummyOperator(
        task_id='dummy_command',
        dag=dag
    )

    def python_count():
        print('Look, I can count from 1 to 10: \n')
        for i in list(range(1, 11)):
            print(i)

    python_count_command = PythonOperator(
        task_id='python_count_command',
        python_callable=python_count,
        dag=dag
    )

    bash_echo_command >> python_print_command >> dummy_command >> python_count_command
