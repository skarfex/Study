import requests
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


class KislitsynRickAndMortyOperator(BaseOperator):

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def get_page_count(self, api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("Success")
            return (int(r.json().get('info').get('pages')))
        else:
            logging.warning("HTTP CODE {}".format(r.status_code))
            raise AirflowException('Error in load page')

    def get_top_locations(self):
        result = list()
        api_url = f'https://rickandmortyapi.com/api/location'
        for i in range(1, self.get_page_count(api_url)):
            url = f'https://rickandmortyapi.com/api/location?page={i}'
            r = requests.get(url)
            data = r.json()
            result.extend(data['results'])
        sorted_all_data = sorted(result, key=lambda x: len(x['residents']), reverse=True)[:3]
        top3 = list()
        for i in sorted_all_data:
            top3.append((i.get('id'), i.get('name'), i.get('type'), i.get('dimension'), len(i.get('residents'))))
        return top3

    def execute(self, context):
        top3 = self.get_top_locations()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, f"INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s",
                       top3)
        conn.commit()
        cursor.execute(f'SELECT * FROM {self.table_name}')
        view = cursor.fetchall()
        logging.info(view)

























