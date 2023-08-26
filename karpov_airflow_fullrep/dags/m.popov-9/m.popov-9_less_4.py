from airflow.decorators import dag
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm.popov-9'
}

@dag(dag_id='m.popov_9_less_4', schedule_interval='0 5 * * 1-6', default_args=DEFAULT_ARGS, catchup=True,)
def my_dag():
    # get_articles  = PostgresOperator(
    #     task_id='task_1',
    #     postgres_conn_id='conn_greenplum',
    #     sql="""SELECT heading FROM articles WHERE id = extract (dow from '{{ ds }}'::date)"""
    # )
    #
    # def print_data():
    #     logging.info('{{ ti.xcom_pull(task_ids="task_1")}}')
    #
    # show_results = PythonOperator(
    #     task_id='task_2',
    #     python_callable=print_data
    # )
    def extract_postgres(load_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        curs = conn.cursor()
        curs.execute(f"""SELECT heading FROM articles WHERE id = extract (dow from '{load_date}'::date)""")
        rows = curs.fetchall()
        conn.close()
        return rows

    get_articles = PythonOperator(
            task_id='task_1',
            python_callable=extract_postgres,
        op_kwargs=dict(load_date='{{ ds }}'
        ))

    def print_xcom(**kwargs):
        logging.info(kwargs['templates_dict']['articles'])


    print_articles = PythonOperator(
        task_id='task_2',
        python_callable=print_xcom,
        templates_dict={'articles': '{{ ti.xcom_pull(task_ids="task_1") }}'},
        provide_context=True
    )

    get_articles >> print_articles

my_dag = my_dag()