from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from m_popov_9_plugins.m_popov_rm_loc_operator import MPopov9_ram_locations_operator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago



DEFAULT_ARGS = {
    'owner': 'm.popov-9',
    'start_date':days_ago(1)
}

@dag(dag_id='m.popov_9_less_5', schedule_interval=None, default_args=DEFAULT_ARGS)
def my_dag():
    create_ram_location_table  = PostgresOperator(
        task_id='create_ram_location_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS m_popov_9_ram_location (
            ram_id int,
            name text,
            type text,
            dimension text,
            resident_cnt int,
            UNIQUE(ram_id, name, type));
          """
    )
    get_ram_location = MPopov9_ram_locations_operator(task_id='get_ram_location',
                                       count=3)

    def insert_postgres(rows_to_insert):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        curs = conn.cursor()
        rows_to_insert = rows_to_insert.replace('[','',1).replace(']','',-1)
        curs.execute(f"""truncate m_popov_9_ram_location""")
        curs.execute(f"""insert into m_popov_9_ram_location VALUES {rows_to_insert}""")
        conn.commit()
        curs.close()
        conn.close()

    load_location = PythonOperator(
        task_id='load_location',
        python_callable=insert_postgres,
        op_kwargs=dict(rows_to_insert='{{ ti.xcom_pull(task_ids="get_ram_location") }}')
    )

    create_ram_location_table >> get_ram_location >> load_location

my_dag = my_dag()