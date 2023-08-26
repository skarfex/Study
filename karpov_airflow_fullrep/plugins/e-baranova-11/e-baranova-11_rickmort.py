import json
import logging

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RickMortyOperator(BaseOperator):

    api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def count_pages(self):

        r = requests.get(self.api_url.format(pg='1'))
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def choose_top_locations(self, target=3):

        to_return = []
        for page in range(self.count_pages()):
            r = requests.get(self.api_url.format(pg=str(page+1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                converted_json = json.loads(r.text)['results']
                for location in converted_json:
                    converted_location = {
                        'id': location['id'],
                        'name': location['name'],
                        'type': location['type'],
                        'dimension': location['dimension'],
                        'resident_cnt': len(location['residents'])
                    }
                    to_return.append(converted_location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return sorted(to_return, key=lambda count: count['resident_cnt'], reverse=True)[:target]

    def execute(self, context):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("location_cursor")

        prepare_values = []
        for value in self.choose_top_locations(target=3):
            prepared = f"({value['id']}, {str(value['name'])}, {str(value['type'])}," \
                       f" {str(value['dimension'])}, {value['resident_cnt']})"
            prepare_values.append(prepared)

        sql_query = f'''INSERT INTO "e_baranova_11_ram_location" VALUES {','.join(prepare_values)}'''
        cursor.execute(sql_query)
        conn.commit()

        logging.info("Inserted into Greenplum: " + sql_query)
