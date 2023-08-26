from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_mashkina_11_plugins.v_mashkina_11_ram_operator import MashkinaRickAndMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-mashkina-11',
    'catchup': True,
    'poke_interval': 600
}

TABLE_NAME = 'v_mashkina_11_rickandmorty_top_locations'
CONN_ID = 'conn_greenplum_write'

with DAG("v-mashkina-11_rickandmorty_top_locations",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-mashkina-11']
) as dag:



    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
                CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} 
                (
                id              int PRIMARY KEY,
                name            varchar(100) NOT NULL,
                type            varchar(100) NOT NULL,
                dimension       varchar(100) NOT NULL,
                resident_cnt    int NOT NULL,
                UNIQUE(id, name, type, dimension, resident_cnt)
                )
                DISTRIBUTED BY (id);
            """,
        autocommit=True,
        postgres_conn_id=CONN_ID,
    )

    clean_table = PostgresOperator(
        task_id='clean_table',
        postgres_conn_id=CONN_ID,
        sql=f"TRUNCATE TABLE public.{TABLE_NAME};",
        autocommit=True,
    )

    write_results = MashkinaRickAndMortyOperator(
        task_id='write_results',
        conn_id=CONN_ID,
        table_name=TABLE_NAME
    )



create_table >> clean_table >> write_results
