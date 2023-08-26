from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from s_duzhak_12_plugins.s_duzhak_pushoperator import SergDuzhPushOperator
from s_duzhak_12_plugins.s_duzhak_getoperator import SergDuzhGetOperator

from airflow.operators.dummy import DummyOperator



DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'sduzhak',
    'poke_interval': 600
}


with DAG("sduzhak_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['sduzh']
         ) as dag:

    def create_table_f(tablename, **kwargs):
        # tab_name = kwargs['tablename']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(f"""
            CREATE TABLE IF NOT EXISTS "{tablename}"
                (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    type VARCHAR,
                    dimension VARCHAR,
                    resident_cnt INTEGER
                );
        """)

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_f,
        op_kwargs={"tablename": "s-duzhak-12_ram_location"}
    )

    dummy = DummyOperator(
        task_id='start',
    )
    getter = SergDuzhGetOperator(
        task_id='get_data'
    )
    pusher = SergDuzhPushOperator(
        task_id='push_data',
        data="{{ ti.xcom_pull(task_ids='get_data', key='return_value') }}"
    )

    dummy >> create_table >> getter >> pusher