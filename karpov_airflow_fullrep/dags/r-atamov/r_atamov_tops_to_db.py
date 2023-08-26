"""
Задание
1. Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
2. С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
3. Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""

from r_atamov_plugins.ragim_top_operator import RagimTopLocationsOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.weekday import WeekDay
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator

from click import echo

DEFAULT_ARGUMENTS = {
    'owner': 'r-atamov',
    'poke_interval': 30,
    'start_date': days_ago(2),
}

with DAG(
    'ragim_tops',
    default_args=DEFAULT_ARGUMENTS,
    description='My top resident RAM DAG',
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['ragim_tops'],
) as dag:

    start = DummyOperator(task_id="start")

    def create_table_func():
        # инициализируем хук
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()  # и курсор
            # исполняем sql
            # sql = open('create_ram_location.sql', 'r').read()
            sql = """
                create table IF NOT EXISTS public.r_atamov_ram_location(
                    id serial4 not null,
                    name varchar(255),
                    type varchar(255),
                    dimension varchar(255),
                    resident_cnt int,
                    constraint pk_key PRIMARY KEY (id)
                )            
            """
            cursor.execute(sql)

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_func,
    )

    get_tops = RagimTopLocationsOperator(
        task_id='get_tops',
        n_tops=3,
    )

    def save_tops_func(**kwargs) -> None:
        """
        Save tops locations
        """

        # TODO: Убрать подсчёт residents - len(location['residents'])

        locations = kwargs['ti'].xcom_pull(
            task_ids='get_tops', key='return_value')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("truncate table r_atamov_ram_location")
        for location in locations:
            pg_hook.run(
                f"""
                insert into public.r_atamov_ram_location(name, type, dimension, resident_cnt) 
                values('{location['name']}', '{location['type']}','{location['dimension']}', {len(location['residents'])})
                """
            )

    save_tops = PythonOperator(
        task_id='save_tops',
        provide_context=True,
        python_callable=save_tops_func,
    )

    end = DummyOperator(task_id="end")

    start >> create_table >> get_tops >> save_tops >> end
