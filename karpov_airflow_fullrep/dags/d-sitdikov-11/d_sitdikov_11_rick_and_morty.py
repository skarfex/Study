from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from d_sitdikov_11_plugins.d_sitdikov_11_rick_and_morty_operator import TopLocationsRnM

users_login = 'd-sitdikov-11'
users_login_un = users_login.replace('-', '_')

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': users_login,
    'poke_interval': 600
}

with DAG(f"{users_login_un}_rick_and_morty",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=[users_login]
         ) as dag:

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            f'''
            CREATE TABLE IF NOT EXISTS "{users_login}ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            f'''TRUNCATE TABLE "{users_login}ram_location";'''
        ],
        autocommit=True
    )

    top_locations = TopLocationsRnM(task_id='top_locations')

    create_or_truncate_table >> top_locations