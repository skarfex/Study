from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

import logging
import requests


class RamTop3LocationsOperator(BaseOperator):

    ui_color = '#e0ffff'
    api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        request = requests.get(self.api_url)
        if request.status_code == 200:
            logging.info("SUCCESS")
            page_count = request.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(request.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        locations = []
        for i in range(self.get_page_count(self.api_url.format(pg=1))):
            request = requests.get(self.api_url.format(pg=i + 1))
            if request.status_code == 200:
                logging.info(f"PARSING PAGE: {i+1} - SUCCESS")
                results = request.json()['results']
                for result in results:
                    locations.append(
                        (result['id'], result['name'], result['type'], result['dimension'], len(result['residents'])))
            else:
                logging.warning("HTTP STATUS {}".format(request.status_code))
                raise AirflowException('Error in load locations')

        locations = sorted(locations, key=lambda x: x[4], reverse=True)[:3]
        logging.info('TOP 3 LOCATIONS:')
        for item in locations:
            logging.info(str(item))
        return ', '.join(str(t) for t in locations)