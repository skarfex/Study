import requests
import logging
import json
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class RAMtop3LocationOperator(BaseOperator):
    
    def __int__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def execute(self, context):
        """
        Logging count of Human in Rick&Morty
        """
        rows = []
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                data = json.loads(r.text)
                for i in data['results']:
                    rows.append((i['id'], i['name'], i['type'], i['dimension'], len(i['residents'])))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        df = pd.DataFrame(rows, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        return df.sort_values(by='resident_cnt', ascending=False).head(3).to_dict()