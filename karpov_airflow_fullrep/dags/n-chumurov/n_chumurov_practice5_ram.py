from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from n_chumurov_plugins.n_chumurov_top3_ram_locations import NChumurovRamTopResidentLocationsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-chumurov'
}

def create_table_func():
    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS n_chumurov_ram_location(
            id int PRIMARY KEY NOT NULL,
            name varchar,
            type varchar,
            dimension varchar,
            resident_cnt integer
            
        );
    """
    )
    logging.info('таблица создана')
    conn.commit()
    cursor.close()
    conn.close()

def write_to_db_func(**context):
    top_locations = context["ti"].xcom_pull(key="n_chumurov_ram_locations")


    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
    conn = pg_hook.get_conn()
    cursor = conn.cursor() 

    list_to_insert = []
    for item in top_locations:
        list_of_items = f"({item['id']}, '{item['name']}', '{item['type']}', '{item['dimension']}', {item['resident_cnt']})"
        list_to_insert.append(list_of_items)   


    cursor.execute(
        f"""
        insert into n_chumurov_ram_location (
            id,
            name,
            type,
            dimension,
            resident_cnt
        )
        values {",".join(list_to_insert)}  
    """
    )
    conn.commit()
    cursor.close()
    conn.close()

    
with DAG("n-chumurov_practice5",
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chumurov']
        ) as dag:

    create_table = PythonOperator(
        task_id="create_table", python_callable=create_table_func
    )

    get_top_3_locations = NChumurovRamTopResidentLocationsOperator(
    task_id='get_top_3_locations', 
     top_size = 3)

    write_to_db = PythonOperator(
        task_id='write_to_db',
        python_callable=write_to_db_func )

    create_table >> get_top_3_locations >> write_to_db



