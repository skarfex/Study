from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from a_chigrinova_13_plugins.a_chigrinova_13_ram_location_operator import ChigrinovaLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-chigrinova-13',
    'poke_interval': 600,
}

with DAG("a-chigrinova-13_plugin_task",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-chigrinova-13']
         ) as dag:

    create_pivot_table = PostgresOperator(
        task_id='create_pivot_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS a_chigrinova_13_ram_location (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT NOT NULL);
        """,
        autocommit=True,
        dag=dag
    )

    truncate_records_for_rewrite = PostgresOperator(
        task_id='truncate_records_for_rewrite',
        postgres_conn_id='conn_greenplum_write',
        sql=f"TRUNCATE TABLE a_chigrinova_13_ram_location;",
        autocommit=True,
        dag=dag
    )

    insert_values_into_table = ChigrinovaLocationOperator(
        task_id='insert_values_into_table',
        dag=dag
    )

    create_pivot_table >> truncate_records_for_rewrite >> insert_values_into_table
