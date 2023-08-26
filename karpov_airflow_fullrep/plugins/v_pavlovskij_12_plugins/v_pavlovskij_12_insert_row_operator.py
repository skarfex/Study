from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class VPavlovskijInsertRowOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        engine = pg_hook.get_sqlalchemy_engine()
        context['ti'].xcom_pull(task_ids='get_locations', key='table').to_sql(
            name='v_pavlovskij_12_ram_location',
            schema='public',
            con=engine,
            if_exists='replace',
            index=False
        )
