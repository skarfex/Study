"""
Загрузка топ Рик и Морти
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import json
import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2), # дата начала генерации DAG Run-ов
    'owner': 'n-novikova-16',  # владелец
    'poke_interval': 600       # задает интервал перезапуска сенсоров (каждые 600 с.)
}

with DAG("n-novikova-lesson-5", # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='@daily', # расписание
    default_args=DEFAULT_ARGS,  # дефолтные переменные
    max_active_runs=1,          # позволяет держать активным только один DAG Run
    tags=['n-novikova-16']      # тэги
) as dag:

    def select_location_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        cursor.execute('SELECT count(*) FROM public.n_novikova_16_ram_location')  # исполняем sql
        res_cnt = cursor.fetchone()[0]  # Одно значение
        logging.info(f"Количество строк: '{res_cnt}'")

        if res_cnt == 0:
            result_query = 'insert_table'
        else:
            result_query = 'not_insert_table'
        return result_query

    def three_top_locations_func(**kwargs):
        # Обращаемся к пути, где лежат данные
        r = requests.get('https://rickandmortyapi.com/api/location')

        # Конвертируем в джейсон и берем results, где лежит инфа о персонажах, в том числе - локации
        locations = json.loads(r.text)['results']

        location_list = []  # Запишем всё в словарь

        # Проходимся по массиву
        for i in locations:
            person = {
                'id': i['id'],
                'name': i['name'],
                'type': i['type'],
                'dimension': i['dimension'],
                'resident_cnt': len(i['residents'])
            }
            location_list.append(person)

        # Нужно отсортировать и взять максимальные
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]

        # Создаем строки для вставки
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in top3_location]

        # Вставляем значения в таблицу
        insert_sql = f"INSERT INTO public.n_novikova_16_ram_location VALUES {','.join(insert_values)}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(insert_sql, False)

    insert_table = PythonOperator(
        task_id='insert_table',
        python_callable=three_top_locations_func,
        dag=dag
    )

    def not_insert_table_func():
        logging.info("В таблице уже есть записи.")

    not_insert_table = PythonOperator(
        task_id='not_insert_table',
        python_callable=not_insert_table_func,
        dag=dag
    )

    select_location = BranchPythonOperator(
        task_id='select_location',
        python_callable=select_location_func,
        dag = dag
    )

    select_location >> [insert_table, not_insert_table]
