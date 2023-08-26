"""
Простой даг для пробы
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import pendulum
import logging

from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook  # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'owner': 'd-severinets',
    'poke_interval': 600
}

with DAG("d_sever_heading_dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         # max_active_runs=1,
         catchup=True,
         tags=['d-severinets']
         ) as dag:
    dummy = DummyOperator(task_id='dummy')

    echo_dsever = BashOperator(
        task_id='echo_dsever',
        bash_command='echo "Hello World!" ',
        dag=dag
    )


    def get_week_day(str_date):
        return pendulum.from_format(str_date, 'YYYY-MM-DD').weekday() + 1


    def log_ds_func(**kwargs):
        tsk_owner = kwargs['templates_dict']['task.owner']
        logging.info('task_owner:' + tsk_owner)
        logging.info('context, {{ ds }}: ' + kwargs['ds'])
        logging.info('day of week, {{ ds }}: ', kwargs['ds'])
        logging.info('context, {{tomorrow_ds}}: ' + kwargs['tomorrow_ds'])
        logging.info('conf:' + kwargs['templates_dict']['config'])


    log_dsever = PythonOperator(
        task_id='log_dsever',
        python_callable=log_ds_func,
        templates_dict={'task.owner': '{{ task.owner }}',
                        'config': '{{ conf }}'},
        dag=dag
    )


    def get_heading_func(**context):
        week_day = get_week_day(context['ds'])
        get_article_sql = f"SELECT heading FROM articles WHERE id = {week_day}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(get_article_sql)
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('query result: ', one_string)


    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_func,
        dag=dag
    )

    dummy >> [echo_dsever, log_dsever] >> get_heading
