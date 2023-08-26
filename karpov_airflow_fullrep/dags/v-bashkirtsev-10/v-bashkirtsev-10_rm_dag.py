from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from v_bashkirtsev_10_plugins.v_bashkirtsev_10_loc_operator import LocOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'poke_interval': 600,
    'owner': 'v-bashkirtsev-10'
}

with DAG("v-bashkirtsev-10_rm_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['v-bashkirtsev-10'],
         max_active_runs=1) as dag:

    get_loc = LocOperator(
         task_id='get_loc'
    )


    def load_to_gp(ti):
        """Заливка в GP"""

        res_loc = ti.xcom_pull(key='return_value', task_ids='get_loc')
        rows = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                for loc in res_loc]
        logging.info(rows)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        create_table = f"""
            CREATE TABLE IF NOT EXISTS v_bashkirtsev_10_ram_location (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
            """
        pg_hook.run(create_table, False)
        truncate_sql = """TRUNCATE TABLE v_bashkirtsev_10_ram_location"""
        pg_hook.run(truncate_sql, False)
        insert_sql = f"INSERT INTO v_bashkirtsev_10_ram_location VALUES {','.join(rows)}"
        pg_hook.run(insert_sql, False)

    load_to_gp = PythonOperator(
         task_id='load_to_gp',
         python_callable=load_to_gp
    )

    get_loc >> load_to_gp
