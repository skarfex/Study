"""
DAG из задания 3 урока - "Сложные пайплайны, часть 1"

5. Внутри создать даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты

"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'start_date': days_ago(2),  # Дата начала выполнения дага
    'owner': 'd-fedoseev',  # Владелец
    'poke_interval': 600  # Интервал проверки между выполнениями задач
}

with DAG("d-fedoseev-task1",
         schedule_interval='@daily',  # Интервал выполнения
         default_args=default_args,
         max_active_runs=1,  # Максимальное кол-во запущенных параллельно дагов
         tags=['d-fedoseev']
         ) as dag:

    start_task = DummyOperator(
        task_id="start_task",
        dag=dag

    )  # DummyOperator - оператор для создания пустой задачи _start

    dummy_task_run = DummyOperator(
        task_id="dummy_task_run",
        dag=dag
    )  # DummyOperator - оператор для создания пустой задачи _dummy_task_run

    dummy_end_task = DummyOperator(
        task_id="dummy_end_task",
        dag=dag
    )  # DummyOperator - оператор для создания пустой задачи _dummy_end_task


    bash_task_date_1 = BashOperator(
        task_id='bash_task_date_1',
        bash_command='echo {{ ds }}',
        dag=dag
    ) # BashOperator - тестирую Jinja

    bash_task_date_2 = BashOperator(
        task_id='bash_task_date_2',
        bash_command='echo $(date +"%Y-%m-%d")',
        dag=dag
    )

    echo_pwd = BashOperator(
        task_id='echo_pwd',
        bash_command='echo pwd',
        dag=dag
    ) # BashOperator - тестирую вывод директории

    def return_42():
        result = 42
        return result

    return_number = PythonOperator(
        task_id='return_number',
        python_callable=return_42,
        dag=dag
    )

    def print_number(number_for_print):
        print(f"number_f_print_output: {number_for_print}")

    print_f = PythonOperator(
        task_id='print_f',
        python_callable=print_number,
        op_args=[return_number.output],
        dag=dag
    )

    def print_date(*args):
        execution_date = args[0]
        print(f"Текущая дата: {execution_date}")

    print_date_task_python_operator = PythonOperator(
        task_id='print_date_task_python_operator',
        python_callable=print_date,
        op_args=['{{ execution_date }}'],
        dag=dag
    )



    start_task >> [bash_task_date_1, bash_task_date_2, print_date_task_python_operator, dummy_task_run] >> print_f >> [echo_pwd, dummy_end_task]