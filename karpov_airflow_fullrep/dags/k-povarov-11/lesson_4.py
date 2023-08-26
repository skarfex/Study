from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    #'end_date': datetime(2022, 3, 15),
    'owner': 'k-povarov-11',
    'poke_interval': 600,
    'retries': 3,
    'retry_delay': 10,
    'priority_weight': 2
}

with DAG("dag_povarov",
          schedule_interval='0 18 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['povarov_dag']
          ) as dag:

    def load_heading_to_xcom_func(current_date, **kwargs):

        day = datetime.strptime(current_date, "%Y-%m-%d").isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("get_heading_cursor")
        cursor.execute("SELECT heading FROM articles WHERE id = {}".format(day))
        one_string = cursor.fetchone()
        conn.close()

        kwargs['ti'].xcom_push(value=one_string, key='today_heading')
        kwargs['ti'].xcom_push(value=str(kwargs['ds']), key='ds')

    load_heading_to_xcom = PythonOperator(
        task_id='load_heading_to_xcom',
        op_kwargs={'current_date': '{{ ds }}'},
        python_callable=load_heading_to_xcom_func,
        provide_context=True
    )

load_heading_to_xcom