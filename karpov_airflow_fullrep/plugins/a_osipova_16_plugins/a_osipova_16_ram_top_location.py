from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

import logging
import requests
import json


class RickAndMortyTopLocationOperator(BaseOperator):
    api_url = 'https://rickandmortyapi.com/api/location?page={page}'

    # template_fields = ('top_location',)
    ui_color = "#e0ffff"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_page_count(self):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(self.api_url.format(page='1'))
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top_locations(self, target=3):

        locations = []
        for page in range(self.get_page_count()):
            r = requests.get(self.api_url.format(page=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                new_json = json.loads(r.text)['results']
                for loc in new_json:
                    new_loc = {'id': loc['id'], 'name': loc['name'], 'type': loc['type'], 'dimension': loc['dimension'],
                               'resident_cnt': len(loc['residents'])}

                    locations.append(new_loc)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load API')
        top = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[:target]
        logging.info(f'top {top}')
        return top

    def execute(self, context):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        data = []
        for v in self.get_top_locations(target=3):
            values = f"({v['id']}, \'{str(v['name'])}\', \'{str(v['type'])}\', \'{str(v['dimension'])}\', {v['resident_cnt']})"

            data.append(values)

        sql = f'''insert into PUBLIC.A_OSIPOVA_16_RAM_LOCATION values {','.join(data)}'''
        # logging.info(sql)
        cursor.execute(sql)
        conn.commit()

        logging.info("GreenPlum: " + sql)