import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def sort_custom_key(val) -> int:
    return val[4]

class SNikulinaRamMortyLocationsOperator(BaseOperator):
    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context) -> any:
        # Проверка подключения к базе данных Greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        try:
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            logging.info(f'Connected to Greenplum: {result}')
        except Exception as e:
            logging.error(f'Error connecting to Greenplum: {str(e)}')
            raise AirflowException('Error connecting to Greenplum')

        locations_list = []
        for page in range(1, self.get_page_count() + 1):
            r = requests.get(self.url + f'/?page={str(page)}')
            if r.status_code == 200:
                results = r.json().get('results')
                items = [
                    [item['id'], item['name'], item['type'], item['dimension'], len(item['residents'])]
                    for item in results
                ]
                logging.info(f'Page {page} processed successfully')
                locations_list.extend(items)
            else:
                logging.warning(f"HTTP STATUS {r.status_code}")
                raise AirflowException('Error occurred while loading data from Rick&Morty API')

        locations_list.sort(key=sort_custom_key, reverse=True)
        logging.info(f'Top 3 locations: {locations_list[:3]}')
        logging.info(f'Locations list received')
        logging.info(f'Execution completed')
        return locations_list[0:3]

    def get_page_count(self) -> int:
        r = requests.get(self.url)
        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            logging.info(f'Page count: {page_count}')
            return page_count
        else:
            print('Error occurred while getting page count')





