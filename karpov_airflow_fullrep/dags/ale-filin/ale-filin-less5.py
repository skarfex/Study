from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ale_filin_plugins.rnm_operator import RnMLocationsOperator
from datetime import datetime

default_args = {
    'owner': 'ale-filin',
    'start_date': datetime(2022,1,1),
    'schedule_interval': None,
    'depends_on_past': False
}

@dag(
    dag_id='ale-filin-less5',
    description='Test dag for rick and morty api',
    default_args=default_args,
    catchup=False
)
def ale_filin_dag():
    
    @task(
        task_id='create-rnm-table'
    )
    def create_rnm_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        print("Creating TABLE")
        cursor.execute(f"""create table if not exists public.ale_filin_rnm_location(
                    id				integer primary key,
                    name			text,
                    type			text,
                    dimension		text,
                    resident_cnt	integer,
                    op_time		timestamp default CURRENT_TIMESTAMP
                );""")
        print("Clearing TABLE")
        cursor.execute("truncate table public.ale_filin_rnm_location;")
        conn.commit()

    rnm_task = RnMLocationsOperator(task_id="gather-rnm-locations", top_limit = 3)

    @task(
        task_id='save-top-locations'
    )
    def save_top_locations(**kwargs):
        ti = kwargs['ti']
        sql_values = ti.xcom_pull(task_ids='gather-rnm-locations')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        print("Loading TABLE")
        cursor.execute(f"""INSERT INTO public.ale_filin_rnm_location(id, "name", "type", dimension, resident_cnt) VALUES\n{sql_values};""")
        conn.commit()

    create_rnm_table() >> rnm_task >> save_top_locations()

dag = ale_filin_dag()
