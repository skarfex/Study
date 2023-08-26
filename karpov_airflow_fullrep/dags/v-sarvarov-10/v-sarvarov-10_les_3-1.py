"""
Тестовый даг Урок 3.1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-sarvarov-10',
    'poke_interval': 600
}

with DAG("vs_test_lesson_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-sarvarov-10']
) as dag:

    dummy_operator = DummyOperator(task_id="dummy")

    echo_operator_1 = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    template_str = """
       
     z$$$$$. $$
    $$$$$$$$$$$
   $$$$$$**$$$$             eeeeer
  $$$$$%   '$$$             $$$$$F
 4$$$$P     *$$             *$$$$F
 $$$$$      '$$    .ee.      ^$$$F            ..e.
 $$$$$       ""  .$$$$$$b     $$$F 4$$$$$$   $$$$$$c
4$$$$F          4$$$""$$$$    $$$F '*$$$$*  $$$P"$$$L
4$$$$F         .$$$F  ^$$$b   $$$F  J$$$   $$$$  ^$$$.
4$$$$F         d$$$    $$$$   $$$F J$$P   .$$$F   $$$$
4$$$$F         $$$$    3$$$F  $$$FJ$$P    4$$$"   $$$$
4$$$$F        4$$$$    4$$$$  $$$$$$$r    $$$$$$$$$$$$
4$$$$$        4$$$$    4$$$$  $$$$$$$$    $$$$********
 $$$$$        4$$$$    4$$$F  $$$F4$$$b   *$$$r
 3$$$$F       d$$$$    $$$$"  $$$F *$$$F  4$$$L     .
  $$$$$.     d$$$$$.   $$$$   $$$F  $$$$.  $$$$    z$P
   $$$$$e..d$$$"$$$b  4$$$"  J$$$L  '$$$$  '$$$b..d$$
    *$$$$$$$$$  ^$$$be$$$"  $$$$$$$  3$$$$F "$$$$$$$"
     ^*$$$$P"     *$$$$*    $$$$$$$   $$$$F  ^*$$$"
                  
                          """
    
    
    def print_template_func(print_this):
        logging.info(print_this)

    print_logo = PythonOperator(
        task_id='print_logo',
        python_callable=print_template_func,
        op_args=[template_str]
    )

    echo_operator_2 = BashOperator(
        task_id='echo_ts',
        bash_command='echo {{ ts }}',
        dag=dag
    )

    dummy_operator >> [echo_operator_1, print_logo, echo_operator_2]
