from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import logging
from airflow.hooks.postgres_hook import PostgresHook
import requests


def get_top(top: int = 3) -> list:
    pg = 1
    base_url = f'https://rickandmortyapi.com/api/location/?page={pg}'
    response = requests.get(base_url)
    count = response.json().get('info').get('pages')
    results_p = response.json().get('results')

    v2 = []
    i = 1
    while i <= count:
        base_url = f'https://rickandmortyapi.com/api/location/?page={i}'
        response = requests.get(base_url)
        results_p = response.json().get('results')
        i += 1
        for loc in results_p:
            v2 += [(loc.get('id'), '\'' + loc.get('name') + '\'', '\'' + loc.get('type') + '\'',
                    '\'' + loc.get('dimension') + '\'', len(loc.get('residents')))]

    v3 = sorted(v2, key=lambda s: s[4], reverse=True)
    v4 = []
    v5 = []
    for item in zip(v3, range(top)):
        v4 += item
        v4.pop(1)
        v5 += v4
        v4 = []
    return v5


class tretyakovLocationsTopOperator (BaseOperator):
    template_fields = ('top_number'),

    ui_color = '#fffacd'

    def __init__(self, top_number: int =3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_number = top_number

    def execute(self, context):
        top_number = self.top_number
        r = get_top(top_number)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        for item in r:
            v1 = item
            cursor.execute(f'CREATE TABLE IF NOT EXISTS a_tretjakov_19_ram_location(id int UNIQUE, name varchar, type varchar, dimension varchar, resident_cnt int)')
            logging.info(f'list field all {item}')
            logging.info(f'INSERT INTO a_tretjakov_19_ram_location values({v1[0]},{v1[1]},{v1[2]},{v1[3]},{v1[4]})')
            cursor.execute(f'INSERT INTO a_tretjakov_19_ram_location values({v1[0]},{v1[1]},{v1[2]},{v1[3]},{v1[4]})')
            conn.commit()

        conn.close()


