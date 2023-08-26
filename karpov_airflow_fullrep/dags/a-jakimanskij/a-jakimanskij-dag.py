"""

Задание (из LMS):

Нужно доработать даг, который вы создали на прошлом занятии.
Он должен:
- Работать с понедельника по субботу, но не по воскресеньям 
- Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

Данный ДАГ создан в рамках выполнения 
задания 3го урока курса Data Engineer.
10ый поток, 2022 год.

Автор - Якиманский А. В.
"""

import logging
from airflow import DAG
from airflow.utils.dates import days_ago
import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


# ██████╗  █████╗  ██████╗     ██████╗ ███████╗███████╗██╗███╗   ██╗██╗████████╗██╗ ██████╗ ███╗   ██╗
# ██╔══██╗██╔══██╗██╔════╝     ██╔══██╗██╔════╝██╔════╝██║████╗  ██║██║╚══██╔══╝██║██╔═══██╗████╗  ██║
# ██║  ██║███████║██║  ███╗    ██║  ██║█████╗  █████╗  ██║██╔██╗ ██║██║   ██║   ██║██║   ██║██╔██╗ ██║
# ██║  ██║██╔══██║██║   ██║    ██║  ██║██╔══╝  ██╔══╝  ██║██║╚██╗██║██║   ██║   ██║██║   ██║██║╚██╗██║
# ██████╔╝██║  ██║╚██████╔╝    ██████╔╝███████╗██║     ██║██║ ╚████║██║   ██║   ██║╚██████╔╝██║ ╚████║
# ╚═════╝ ╚═╝  ╚═╝ ╚═════╝     ╚═════╝ ╚══════╝╚═╝     ╚═╝╚═╝  ╚═══╝╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
                                                                                                    

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'a-jakimanskij',
    'poke_interval': 600
}

with DAG(
    dag_id='a-jakimanskij-leson-4',
    description="Даг для 4го урока",
    default_args=DEFAULT_ARGS,  # передаем аргументы в ДАГ
    schedule_interval='0 0 * * 1-6', # Кроме воскресенья
    max_active_runs=1,          
    tags=['a-jakimanskij',]      # пометим наш ДАГ
) as dag:

    # ==================================

    def get_gp_cursor():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        return cursor

    def get_article_by_id(article_id):
        logging.info(f'Executing get_article_by_id with article_id={article_id}')
        cursor = get_gp_cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        result =  cursor.fetchone()[0]
        logging.info(f'Result is: {result}')

    get_article = PythonOperator(
        task_id='get_article_by_id',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    get_article

    # ==================================

# ███████╗███████╗████████╗██╗   ██╗██████╗     ██████╗  ██████╗  ██████╗███████╗
# ██╔════╝██╔════╝╚══██╔══╝██║   ██║██╔══██╗    ██╔══██╗██╔═══██╗██╔════╝██╔════╝
# ███████╗█████╗     ██║   ██║   ██║██████╔╝    ██║  ██║██║   ██║██║     ███████╗
# ╚════██║██╔══╝     ██║   ██║   ██║██╔═══╝     ██║  ██║██║   ██║██║     ╚════██║
# ███████║███████╗   ██║   ╚██████╔╝██║         ██████╔╝╚██████╔╝╚██████╗███████║
# ╚══════╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝         ╚═════╝  ╚═════╝  ╚═════╝╚══════╝
                                                                  
dag.doc_md = __doc__
