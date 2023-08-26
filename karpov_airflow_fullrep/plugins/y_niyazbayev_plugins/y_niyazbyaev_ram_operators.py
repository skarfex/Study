import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

def sort_custom_key(val) -> int:
    return val[4]

class YerzhanRamTop3LocationOperator(BaseOperator):

    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context) -> any:

        locations_list = []
        for page in range(1, self.get_page_count() + 1):
            r = requests.get(self.url + f'/?page={str(page)}')
            if r.status_code == 200:
                results = r.json().get('results')
                items = [[item['id'], item['name'], item['type'], item['dimension'], len(item['residents'])] for item in
                         results]
                logging.info(f'PAGE {page} complete')
                locations_list.extend(items)
            else:
                logging.warning(f"HTTP STATUS {r.status_code}")
                raise AirflowException('Error in load from Rick&Morty API')

        locations_list.sort(key=sort_custom_key, reverse=True)
        logging.info(f'{locations_list[:3]}')
        logging.info(f'locations_list received')
        logging.info(f'execute completed')
        return locations_list[0:3]

    def get_page_count(self) -> int:
        r = requests.get(self.url)
        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count: {page_count}')
            return page_count
        else:
            print('error _get_page_count')