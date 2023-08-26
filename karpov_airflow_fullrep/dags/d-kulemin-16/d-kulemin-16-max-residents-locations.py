"""
Даг записывающий в GreenPlum три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import logging

from d_kulemin_16_plugins.d_kulemin_16_ram_max_residents_locations_operator import DKulemin16NMaxResidentsLocations


DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'd-kulemin-16',
    'poke_interval': 600
}


with DAG(
    "d-kulemin-16-max-residents-locations",
    schedule_interval='0 0 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-kulemin-16']
) as dag:
    get_max_residents_locations = DKulemin16NMaxResidentsLocations(
        task_id='get_max_residents_locations',
        n_max=3,
    )

    create_d_kulemin_16_ram_location_table = PostgresOperator(
        task_id='create_d_kulemin_16_ram_location_table',
        postgres_conn_id = 'conn_greenplum_write',
        sql="""
        create table if not exists d_kulemin_16_ram_location (
            id integer,
            name varchar,
            type varchar,
            dimension varchar,
            resident_cnt integer
        )
        """
    )

    truncate_d_kulemin_16_ram_location_table = PostgresOperator(
        task_id='truncate_d_kulemin_16_ram_location_table',
        postgres_conn_id = 'conn_greenplum_write',
        sql="""
        truncate table public.d_kulemin_16_ram_location
        """
    )

    def insert_locations_data(schema, table, **context):
        pg_hook = PostgresHook('conn_greenplum_write')
        locations = context['ti'].xcom_pull(
            task_ids='get_max_residents_locations', key='d-kulemin-16-locations'
        )
        for location in locations:
            pg_hook.run(
                sql=f"""
                INSERT INTO {schema}.{table} 
                VALUES (
                    {location['id']}, 
                    '{location['name']}', 
                    '{location['type']}', 
                    '{location['dimension']}', 
                    '{location['resident_cnt']}'
                );
                """
            )


    insert_rows_into_d_kulemin_16_ram_location_table = PythonOperator(
        task_id='insert_rows_into_d_kulemin_16_ram_location_table',
        python_callable=insert_locations_data,
        op_args=['public', 'd_kulemin_16_ram_location'],
        provide_context=True
    )

    (
        get_max_residents_locations 
        >> create_d_kulemin_16_ram_location_table 
        >> truncate_d_kulemin_16_ram_location_table 
        >> insert_rows_into_d_kulemin_16_ram_location_table
    )
