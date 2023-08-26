from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from k_nameverchenko_19_plugins.k_nameverchenko_19_rm import KVerchenkoRickMortyOperator

DEFAULT_ARGS = {
    'owner': 'k-nameverchenko-19',
    'start_date': days_ago(2),
    'poke_interval': 300
}

with DAG("kverchenko_5",
    schedule_interval = "@daily",
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['k-nameverchenko-19']
) as dag:

    def create_table_in_gp():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # курсор
        cursor.execute("CREATE TABLE IF NOT EXISTS public.k_nameverchenko_19_ram_location (id INT, name VARCHAR, type VARCHAR, dimension VARCHAR, residents INT)")  # исполняем sql

    def truncate_table():
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")  # инициализируем хук
        pg_hook.run("TRUNCATE TABLE public.k_nameverchenko_19_ram_location", True)

    def write_result_to_gp():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        for i in KVerchenkoRickMortyOperator.api_data:
            pg_hook.run(f'INSERT INTO public.k_nameverchenko_19_ram_location VALUES {i}', False)

    create_table_in_gp = PythonOperator(
        task_id = 'create_table_in_gp',
        python_callable = create_table_in_gp,
        provide_context = True)

    truncate_table = PythonOperator(
        task_id = 'truncate_table',
        python_callable = truncate_table,
        provide_context = True)

    write_result_to_gp = PythonOperator(
        task_id = 'write_result_to_gp',
        python_callable = write_result_to_gp,
        provide_context = True)

    create_table_in_gp >> truncate_table >> write_result_to_gp