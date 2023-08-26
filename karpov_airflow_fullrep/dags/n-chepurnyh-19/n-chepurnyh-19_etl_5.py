"""
> Задание
1. Создайте в GreenPlum'е таблицу с названием n_chepurnyh_19_ram_location"
с полями id, name, type, dimension, resident_cnt.
2. С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти"
с наибольшим количеством резидентов.
3. Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

> hint
pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
pg_hook.run(sql_statement, False)
* Для работы с GreenPlum используется соединение 'conn_greenplum_write' в случае,
если вы работаете с LMS либо настроить соединение самостоятельно в вашем личном Airflow. Параметры соединения:
Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: students (не karpovcourses!!!)
Login: student
Password: Wrhy96_09iPcreqAS
* Можно использовать хук PostgresHook, можно оператор PostgresOperator
* Предпочтительно использовать написанный вами оператор для вычисления top-3 локаций из API
* Можно использовать XCom для передачи значений между тасками, можно сразу записывать нужное значение в таблицу
* Не забудьте обработать повторный запуск каждого таска: предотвратите повторное создание таблицы,
позаботьтесь об отсутствии в ней дублей
"""

import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from n_chepurnyh_19_plugins.n_chepurnyh_19_ram_operator import NChepurnyh19RamLocationOperator
# from n_chepurnyh_19_plugins.n_chepurnyh_19_ram_operator import NChepurnyh19RamSpeciesCountOperator
# from n_chepurnyh_19_plugins.n_chepurnyh_19_ram_operator import NChepurnyh19RamDeadOrAliveCountOperator
# from n_chepurnyh_19_plugins.n_chepurnyh_19_ram_sensor import NChepurnyh19RandomSensor


DEFAULT_ARGS = {'start_date': datetime(2023, 4, 10),
                'end_date': datetime(2023, 4, 20),
                'owner': 'n-chepurnyh-19',
                'poke_interval': 120}
with DAG("n-chepurnyh-19_etl_5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chepurnyh-19']
         ) as dag:
    table = 'n_chepurnyh_19_ram_location'
    conn = 'conn_greenplum_write'

    start = DummyOperator(task_id='start')

    get_top3_location = NChepurnyh19RamLocationOperator(
        task_id='get_top3_location',
        count_top_locations=3,
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=conn,
        trigger_rule='all_success',
        sql=f'''
            CREATE TABLE IF NOT EXISTS {table} (
                        id INT4 PRIMARY KEY,
                        name VARCHAR NOT NULL,
                        type VARCHAR NOT NULL,
                        dimension VARCHAR NOT NULL,
                        resident_cnt INT4 NOT NULL);
            '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id=conn,
        trigger_rule='all_done',
        sql=f'TRUNCATE TABLE {table}'
    )

    def insert_locations_data(**context):
        pg_hook = PostgresHook('conn_greenplum_write')
        locations = context['ti'].xcom_pull(
            task_ids='get_top3_location', key='n_chepurnyh_19_locations'
        )
        logging.info(f'{locations}')
        for location in locations:
            logging.info(f'{location}')
            pg_hook.run(
                sql=f"""
                    INSERT INTO public.{table} 
                    VALUES (
                        {location[0]}, 
                        '{location[1]}', 
                        '{location[2]}', 
                        '{location[3]}', 
                        '{location[4]}'
                    );
                    COMMIT;
                    """
            )

    insert = PythonOperator(
        task_id='insert',
        python_callable=insert_locations_data,
        provide_context=True
    )

    check = PostgresOperator(
        task_id='check',
        postgres_conn_id=conn,
        sql=f'select * from {table};',
        autocommit=True,
    )

start >> get_top3_location >> create_table >> clear_table >> insert >> check


"""
print_alien_count = NChepurnyh19RamSpeciesCountOperator(
    task_id='print_alien_count',
    species_type='Alien'
)

print_dead_count = NChepurnyh19RamDeadOrAliveCountOperator(
    task_id='print_dead_count',
    dead_or_alive='Dead'
)
random_wait = NChepurnyh19RandomSensor(
    task_id='random_wait',
    mode='reschedule',
    range_number=2
)

start >> [print_alien_count, print_dead_count, random_wait]
"""
