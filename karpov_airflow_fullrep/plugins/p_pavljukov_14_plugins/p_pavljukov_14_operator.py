import logging
import csv

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class Ppavlyukov_14Hook(HttpHook):
    """
    Interact with Rick&Morty API.
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']

class PavljukovLocationsOperator(BaseOperator):

    def __init__(self , **kwargs) -> None:
        super().__init__(**kwargs)


    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        locations = []
        hook = Ppavlyukov_14Hook('dina_ram')
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            json = hook.get_page(str(page+1))
            for i in json:
                locations.append({
                    'id': i['id'],
                    'name': i['name'],
                    'type': i['type'],
                    'dimension': i['dimension'],
                    'resident_cnt': len(i['residents'])
                })

        top = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[0:3]
        logging.info(f'top {top}')
        self.xcom_push(context, key='top', value=top)
        return top

