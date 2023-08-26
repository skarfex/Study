import requests
import logging
import pandas as pd
from psycopg2.extras import execute_values
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

class MashkinaRickAndMortyOperator(BaseOperator):

    ui_color = '#fffacd'

    #dataframe.sort_values('resident_cnt', ascending=False).head(3)

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def get_page_count(self, api_url):
        request = requests.get(api_url)

        if request.status_code == 200:
            logging.info("SUCCESS")
            page_count = request.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("Http status {}".format(request.status_code))
            raise AirflowException('Error in load pages')

    def get_locations(self):
        url = 'https://rickandmortyapi.com/api/location'
        results = []
        for i in range(1, self.get_page_count(url)):
            page_url = f'https://rickandmortyapi.com/api/location?page={i}'
            request = requests.get(page_url)
            data = request.json()
            results.extend(data['results'])

        content = sorted(results, key=lambda x: len(x['residents']), reverse=True)[:3]

        top = []
        for i in content:
            top.append(
                (i.get('id'), i.get('name'), i.get('type'), i.get('dimension'), len(i.get('residents')),))
        return top

    def execute(self, context):
        top3 = self.get_locations()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, f"INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s",
                       top3)
        conn.commit()
        cursor.execute(f'SELECT * FROM {self.table_name}')
        query_res = cursor.fetchall()
        logging.info(f'OUTPUT after:\n{query_res}')
