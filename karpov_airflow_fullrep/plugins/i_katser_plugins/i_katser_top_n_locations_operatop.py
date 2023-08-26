from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import requests
import logging
import json

class IKatserTopNLocationsOperator(BaseOperator):
    """
    Вычисление топ-N локаций из API
    """
    template_fields = ('top_locations',)
    ui_color = "#483955"

    def __init__(self, top_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_locations = top_locations

    def count_residents(self, data_json: str) -> tuple:
        #data_json = json.loads(data_json)
        return ({'id': data_json['id'],
                'name': data_json['name'],
                'type': data_json['type'],
                 'dimension': data_json['dimension'],
                'residents': len(data_json['residents'])})


    def execute(self, context):
        """
        Топ-N локаций
        """
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        data_locations = []
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                list_l = r.json().get('results')
                for k in list_l:
                    data_locations.append(self.count_residents(k))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        top_n = sorted(data_locations, key=lambda x: x['residents'], reverse=True)[:self.top_locations]
        return top_n


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