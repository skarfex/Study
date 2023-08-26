"""
ТОП 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from s_dobrynin_plugins.s_dobrynin_ram_top3_op import s_dobrynin_ram_top3_op

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-dobrynin',
    'poke_interval': 600
}

with DAG( "s-dobrynin_ram",
            schedule_interval='@daily',
            default_args=DEFAULT_ARGS,
            max_active_runs=1,
            tags=['s-dobrynin']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")

    table_ram = PostgresOperator(
        task_id='table_ram',
        postgres_conn_id='conn_greenplum_write',
        sql="""
                create table if not exists public.s_dobrynin_ram_location(
                    id int, 
                    name varchar, 
                    type varchar, 
                    dimension varchar, 
                    resident_cnt int
                )
                 distributed randomly;
            """
    )

    trunc  = PostgresOperator(
        task_id='trunc',
        postgres_conn_id='conn_greenplum_write',
        sql = """
                TRUNCATE TABLE public.s_dobrynin_ram_location
                """
    )

    top_3_op = s_dobrynin_ram_top3_op(
        task_id = 'top'

    )

    def insert_table(**kwargs):
        res = kwargs['ti'].xcom_pull(task_ids="top")
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        for i, column in res.iterrows():
            sql = f"""
                     ('{str(column['id'])}', 
                        '{str(column['name'])}', '{str(column['type'])}', '{str(column['dimension'])}', '{str(column['resident_cnt'])}');
                    """
            sql = sql[1:]
            sql2 = f"INSERT INTO  public.s_dobrynin_ram_location VALUES " + sql
            pg_hook.run(sql2, False)

    store = PythonOperator(
        task_id='store',
        python_callable=insert_table
    )

    close = DummyOperator(task_id="close")

    dummy >> table_ram >> top_3_op >> trunc >> store >> close