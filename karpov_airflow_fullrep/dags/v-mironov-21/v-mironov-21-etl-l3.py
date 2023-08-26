from datetime import datetime
from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "start_date": datetime(2022, 3, 1),
    "end_date": datetime(2022, 3, 15),
    "owner" : "v-mironov",
    "poke_interval" : 600
}

def print_date(ds):
    import logging

    logging.info(ds)

def get_from_gp(weekday):
    import logging
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    query = f"SELECT heading FROM articles WHERE id = {weekday}"
    cursor.execute(query)
    query_res = cursor.fetchall()
    logging.info(query_res[0])
    return query_res[0]

with DAG(
    "v-mironov-etl-lesson-3",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * 1-6",
    max_active_runs=1,
    catchup=True,
    tags=["v-mironov"]
) as dag:

    dummy_entrance = DummyOperator(task_id="dummy_entrance", dag=dag)

    bash_date = BashOperator(
        task_id="bash_date",
        bash_command="echo {{ ds }}",
        dag=dag
    )

    python_gp = PythonOperator(
        task_id = "python_gp",
        provide_context=True,
        python_callable=get_from_gp,
        op_args=["{{ dag_run.logical_date.weekday() + 1 }}"],
        dag=dag
    )

    dummy_entrance >> bash_date >>  python_gp
