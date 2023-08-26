"""
находит локации с наибольшим количеством персонажей и
выводит информацию по этим локациям (id, name, type, dimension, residents count)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from i_ilichev_plugins.i_ilichev_ram_top_locations_count_residents import IvanRamTopLocationsCountResidents

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-ilichev',
    'poke_interval': 600,
    'trigger_rule': 'all_success'
}

with DAG("i-ilichev_lesson_5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-ilichev']
         ) as dag:

    # зададим пустой оператор просто для отработки
    dummy = DummyOperator(task_id="dummy")

    # зададим пользовательский оператор, считающий топ локаций
    loc_info = IvanRamTopLocationsCountResidents(
        task_id='loc_info',
        top_locations=3,
        dag=dag
    )

    def info_to_gp_writer_func(**kwargs):

        # затянем топ локаций из XCom
        task_instance = kwargs['task_instance']
        locations = task_instance.xcom_pull(task_ids='loc_info')

        # создадим соединение с бд
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # затянем данные из таблицы
        cursor.execute('SELECT a.id FROM public.i_ilichev_ram_location a')
        selected_rows = cursor.fetchall()

        # список из ids уже существующих в бд записей
        existing_ids = []
        for row in selected_rows:
            existing_ids.append(row[0])
        existing_ids.sort()
        logging.info(f'existing ids: {existing_ids}')

        # список из ids, которые хотим смерджить с уже существующими в бд записями
        ids_to_update = []
        for location in locations:
            ids_to_update.append(location[0])
        ids_to_update.sort()
        logging.info(f'ids to update: {ids_to_update}')

        # удалим из таблицы записи, отсутствующие в списке на обновление
        for ex in existing_ids:
            if ex not in ids_to_update:
                cursor.execute(
                    """
                    DELETE FROM public.i_ilichev_ram_location a
                    WHERE a.id = %s
                    """,
                    (ex,)
                )
                conn.commit()
                logging.info(f'id {ex} deleted from table')
            else:
                pass

        # добавим в таблицу новые записи, отсутствующие в таблице
        # перебираем все ids из списка на обновление
        for up in ids_to_update:
            # оставляем те из ids на обновление, которых нет в списке уже существующих в таблице
            if up not in existing_ids:
                # находим запись, соответствующую id на обновление
                for location in locations:
                    if location[0] == up:
                        cursor.execute('INSERT INTO public.i_ilichev_ram_location VALUES %s', (location,))
                        conn.commit()
                        logging.info(f'id {location[0]} inserted: {location}')
                    else:
                        pass
            else:
                pass

        conn.close()

    # зададим оператор обновления таблицы
    info_to_gp_writer = PythonOperator(
        task_id='info_to_gp_writer',
        python_callable=info_to_gp_writer_func,
        dag=dag,
        provide_context=True
    )

    dummy >> loc_info >> info_to_gp_writer