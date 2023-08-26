from airflow import DAG

import requests
import json

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-zaharova',
    'poke_interval': 600
}

with DAG("a-zaharova_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zaharova']
         ) as dag:
    start = DummyOperator(task_id="start")
    chapter_one = DummyOperator(task_id="chapter_one")


    def select_ram_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("cursor_date")
        cursor.execute('SELECT id FROM a_zaharova_ram_location')
        one_string = cursor.fetchall()
        ids = []
        for a in one_string:
            ids.append(a[0])

        # Make GET request to the API
        response = requests.get('https://rickandmortyapi.com/api/location')

        # Parse the JSON data from the response
        data = json.loads(response.text)

        tmp_dict = {}
        for el in data['results']:
            tmp_dict[el['id']] = [len(el['residents']), el['id'], el['name'], el['type'], el['dimension']]

        tmp_dict = sorted(tmp_dict.items(), key=lambda item: item[1], reverse=True)

        new = []
        c = 0
        for res in tmp_dict:
            c += 1
            if str(res[1][1]) not in ids:

                new.append(
                    '(\'' + str(res[1][1]) + '\',' +
                    '\'' + str(res[1][2]) + '\',' +
                    '\'' + str(res[1][3]) + '\',' +
                    '\'' + str(res[1][4]) + '\',' +
                    '\'' + str(res[1][0]) + '\')')

            if c == 3:
                break
        values = ','.join(new)
        if len(new) > 0:
            pg_hook.run(
                'INSERT INTO a_zaharova_ram_location (id, name, type, dimension, resident_cnt) VALUES ' + values + ';',
                False)


    select_ram = PythonOperator(
        task_id='select_ram',
        python_callable=select_ram_func
    )

    start >> select_ram
