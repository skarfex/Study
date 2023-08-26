from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
import requests
import logging

class SJakovleva20OperatorTopLocations(BaseOperator):

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def get_char_page_count(self, api_url):
            r = requests.get(api_url)
            if r.status_code == 200:
                logging.info("SUCCESS")
                page_count = r.json().get('info').get('pages')
                logging.info(f'page_count = {page_count}')
                return page_count
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load page count')

    def top_locations(self):
        results_loads = []
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_char_page_count(api_url.format(pg='1'))):
            url_page = requests.get(api_url.format(pg=str(page + 1)))
            if url_page.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                my_page = url_page.json()
                results_loads.extend(my_page['results'])
            else:
                logging.warning("HTTP STATUS {}".format(url_page.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        top_locations_result = []
        results_loads_sorted = sorted(results_loads, key=lambda x: len(x['residents']), reverse=True)[:3]
        for memb in results_loads_sorted[:3]:
            top_locations_result.append((memb.get('id'), memb.get('name'), memb.get('type'), memb.get('dimension'), len(memb.get('residents')),))
        return top_locations_result

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, f"INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s", self.top_locations())
        conn.commit()
        logging.info(f'data insert into table')
        cursor.close()
        conn.close