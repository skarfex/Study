import logging
import requests

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

class RamTopLocationOperator(BaseOperator):

    api_url = 'https://rickandmortyapi.com/api/location'

    template_fields = ("top_cnt",)
    ui_color = '#e0ffff'

    def __init__(self, top_cnt: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_cnt = top_cnt

    def execute(self, context):
        r = requests.get(self.api_url)
        if r.status_code == 200:
            logging.info("HTTP STATUS {}".format(r.status_code))
            locations = []
            for record in r.json()['results']:
                 locations.append((record['id'], record['name'], record['type'], record['dimension'], len(record['residents'])))
            locations.sort(key=lambda k: k[-1], reverse=True)
            logging.info('Top three locations:')
            logging.info(locations[:self.top_cnt])
            return locations[:self.top_cnt]
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')
