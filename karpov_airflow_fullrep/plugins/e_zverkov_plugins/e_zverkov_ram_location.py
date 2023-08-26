import logging
import requests
import json

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

class EZVERKOVRickAndMortyTopLocationOperator(BaseOperator):

    api_url = 'https://rickandmortyapi.com/api/location?page={page}'
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
                logging.info("SUCCESS")
                my_json = json.loads(r.text)['results']
                for loc in my_json:
                    loc = {'id': loc['id'], 'name': loc['name'], 'type': loc['type'], 'dimension': loc['dimension'],
                               'resident_cnt': len(loc['residents'])}

                    locations.append(loc)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load API')
        top_locations = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[:target]
        return top_locations

    def execute(self, context):
        """
               Insert top_locations
        """
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        data = []
        for v in self.get_top_locations(target=3):
            values = f"({v['id']}, \'{str(v['name'])}\', \'{str(v['type'])}\', \'{str(v['dimension'])}\', {v['resident_cnt']})"
            data.append(values)
        #print(data)
        logging.info(f'data = {data}')

        sql = f'''insert into public.e_zverkov_ram_location VALUES {','.join(data)}'''
        logging.info(sql)
        cursor.execute(sql)
        conn.commit()


