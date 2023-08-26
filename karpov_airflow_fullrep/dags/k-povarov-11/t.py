from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime


schedule_interval = '0 0 * * 1-6'

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'k-povarov-11',
    'poke_interval': 600
}


with DAG( dag_id="unloading_from_gp_povarov",
          schedule_interval=schedule_interval,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['povarov_4']
          ) as dag:

    def load_from_gp_func(carrent_day, **kwargs):
        day = datetime.strptime(carrent_day, "%Y-%m-%d").isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('select heading from articles  where id = {}'.format(day))
        query_res = cursor.fetchall()[0]
        conn.close()

        kwargs['ti'].xcom_push(key='heading', value=query_res)
        kwargs['ti'].xcom_push(key='ds', value=str(kwargs['ds']))

    load_xcom_heading = PythonOperator(
        task_id='load_xcom_heading',
        op_kwargs={'carrent_day': "{{ ds }}"},
        python_callable=load_from_gp_func,
        provide_context=True
    )

load_xcom_heading