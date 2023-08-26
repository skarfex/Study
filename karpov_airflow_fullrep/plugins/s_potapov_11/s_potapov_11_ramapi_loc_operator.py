from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

import requests
import logging




class GetTopLocationOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_url = 'https://rickandmortyapi.com/api/location/'



    def __get_page_count(self):
        response = requests.get(self.base_url)
        if response.status_code == 200:
            logging.info('First request - Success')
            self.page_count = int(response.json().get('info').get('pages'))
            logging.info(f'page count = {self.page_count}')
        else:
            logging.warning(f"HTTP STATUS {response.status_code}")
            raise AirflowException('Error in load page count')

    def __get_all_locations(self):
        self.__get_page_count()
        self.results = []
        for page in range(1, self.page_count + 1):
            response = requests.get(self.base_url + f'?page={page}')
            if response.status_code == 200:
                logging.info(f'PAGE {page} load Success')
                results = response.json()['results']

                for r in results:
                    self.results.append((len(r['residents']), r['id'], r['name'], r['type'], r['dimension']))
            else:
                logging.warning(f"Problem with load page {page}. Error status {response.status_code}.")
                raise AirflowException('Error in load page count')

    def get_top_locations(self):
        self.__get_all_locations()
        top_3_locations = sorted(self.results, key=lambda x: x[0], reverse=True)[:3]
        return ','.join(map(str, top_3_locations))

    def execute(self, context):
        return self.get_top_locations()