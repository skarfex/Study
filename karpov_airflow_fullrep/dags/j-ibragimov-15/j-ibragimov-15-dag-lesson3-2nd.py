"""
j-ibragimov-15 lesson 3 2nd part
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'j-ibragimov-15',
    #'start_date': days_ago(0),
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'poke_interval': 600  
}

def christmas_tree(**kwargs):
    week_day = datetime.datetime.today().weekday() + 1
    logging.info("\n" + "\n".join([f"{'*' * (2 * n + 1):^{2 * week_day + 1}}" for n in (*range(week_day), 0, 0)])),
    kwargs['ti'].xcom_push(key='j-ibragimov-15_week_day', value=week_day)

with DAG("yi_lesson3_2",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=['j-ibragimov-15'],
    user_defined_macros={'christmas_tree': christmas_tree}
) as dag:

    dummy = DummyOperator(
        task_id='upstream_check',
        trigger_rule='all_success' 
    )
           
    christmas_tree = PythonOperator(
        task_id='christmas_tree',
        python_callable=christmas_tree,
        provide_context=True
    )

    def get_heading_from_articles(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        week_day = kwargs['ti'].xcom_pull(task_ids='christmas_tree', key='j-ibragimov-15_week_day')
        cursor.execute("select heading from articles where id = " + str(week_day))
        query_res = cursor.fetchone()[0]     
        logging.info(query_res)

    get_heading = PythonOperator(
        task_id="get_heading",
        python_callable=get_heading_from_articles,
        provide_context=True        
    )

    weekday = BashOperator(
        task_id='weekday',
        bash_command="echo Today number of week is {{execution_date.strftime('%w')}}"
    )
     

    christmas_tree >> get_heading >> weekday >> dummy