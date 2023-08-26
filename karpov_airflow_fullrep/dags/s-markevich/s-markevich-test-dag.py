"""
    test PostgresOperator
"""

import pendulum
import logging

from datetime import timedelta
from airflow import DAG

from airflow.utils.dates import days_ago

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def logging_args_kwargs(kwargs):
    logging.info('//////////////////////////////////////////////////////////////////////')
    logging.info('op_kwargs, №1: ' + kwargs['kwarg1'] + type(kwargs['kwarg1']))
    logging.info('op_kwargs, №2: ' + kwargs['kwarg2'] + type(kwargs['kwarg2']))
    logging.info('op_kwargs, №1: ' + kwargs['kwarg_pull1'] + type(kwargs['kwarg_pull1']))
    logging.info('op_kwargs, №2: ' + kwargs['kwarg_pull2'] + type(kwargs['kwarg_pull2']))
    logging.info('template_dict' + kwargs['templates_dict'])
    logging.info('//////////////////////////////////////////////////////////////////////')


def test_templates_and_xcom(kwargs):
    my_test_var_json = []
    for x in range(5):
        my_test_var_json.append({
            "id": x,
            "name": "*" * x,
        })
    my_test_var_dict = my_test_var_json[0]
    kwargs['ti'].xcom_push(value=my_test_var_dict, key='dict')
    kwargs['ti'].xcom_push(value=my_test_var_json, key='json')

    template_values = "({{id}}, {{name}})"
    values = ',\n'.join([template_values.format(**row) for row in my_test_var_json])
    kwargs['ti'].xcom_push(value=values, key='values')

    logging_args_kwargs(kwargs)


def print_param_func(**kwargs):
    logging_args_kwargs(kwargs)
    for key, var in kwargs.items():
        logging.info(f'/////////// {key} == type( {type(var)} ) == {var} /////////////')


DEFAULT_ARGS = {
    'owner': 's-markevich',
    'start_date': days_ago(2),
    'poke_interval': 600,
    'priority_weight': 100,
}

with DAG("s_markevich_test_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['markevich_test'],
         render_template_as_native_obj=True
         ) as dag:

    push_param = PythonOperator(
        task_id='push_param',
        python_callable=test_templates_and_xcom,
        provide_context=True,
        op_kwargs={
            'kwarg1': '{{ macros.datetime.now() }}',
            'kwarg2': '{{ var.value }}',
            'kwarg_pull1': '{{ var.value }}',
            'kwarg_pull2': '{{ var.json }}'
        },
        templates_dict={
            'kwarg1': '{{ macros.datetime.now() }}',
            'kwarg2': '{{ var.value }}',
            'kwarg_pull1': '{{ var.value }}',
            'kwarg_pull2': '{{ var.json }}'
        },
    )

    print_param = PythonOperator(
        task_id='print_param',
        python_callable=print_param_func,
        provide_context=True,
        op_kwargs={
            'kwarg1': '{{ macros.datetime.now() }}',
            'kwarg2': '{{ var }}',
            'kwarg_pull1': '{{ ti.xcom_pull(task_ids="push_param", key="dict") }}',
            'kwarg_pull2': '{{ ti.xcom_pull(task_ids="push_param", key="json") }}'
        },
        templates_dict={
            'kwarg1': '{{ macros.datetime.now() }}',
            'kwarg2': '{{ var }}',
            'kwarg_pull1': '{{ ti.xcom_pull(task_ids="push_param", key="dict") }}',
            'kwarg_pull2': '{{ ti.xcom_pull(task_ids="push_param", key="json") }}'
        },
    )

    data_to_greenplum = PostgresOperator(
        task_id='data_to_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            insert into s_markevich_ram_location_for_test_dag
                (id, name)
            values
                 {{ ti.xcom_pull(task_ids='push_param', key='values') }}
            ;
        """,
    )

    push_param >> print_param >> data_to_greenplum
