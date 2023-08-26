import logging

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
class ASyromyatnikovRamLocationOperator(BaseOperator):

    def __int__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_location_info_on_page(self, result_json: list) -> int:
        location_info_on_page = []
        for record in result_json:
            location_info_on_page.append((
                record.get('id'),
                record.get('name'),
                record.get('type'),
                record.get('dimension'),
                len(record.get('residents'))
            ))
        return location_info_on_page

    def get_location_info(self):
        location_info = []
        location_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(location_url.format(pg='1'))):
            r = requests.get(location_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'Page {page + 1}')
                location_info.extend(self.get_location_info_on_page(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return location_info

    def top_3_locations(self):
        location_data = self.get_location_info()
        top_cnt_location = sorted(location_data, key=lambda x: x[4], reverse=True)
        return top_cnt_location[:3]

    def execute(self, context):
        data = self.top_3_locations()
        data_to_insert = ','.join(map(str, data))
        sql_query = f'INSERT INTO a_syromyatnikov_14_ram_location (id, name, type, dimension, resident_cnt) VALUES {data_to_insert}'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(sql_query, False)
