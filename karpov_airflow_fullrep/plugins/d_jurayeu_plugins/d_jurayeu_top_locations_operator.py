import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class DJurayeuTopLocationsOperator(BaseOperator):
    """
    Поиск топ-3 локаций сериала "Рик и Морти" с наибольшим количеством резидентов
    """

    ui_color = "#596275"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self):
        """
        Get top-3 locations in Rick&Morty
        """
        locations_info = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations_list = r.json().get('results')
                for location in locations_list:
                    temp_dict = {}
                    temp_dict['id'] = location.get('id')
                    temp_dict['name'] = location.get('name')
                    temp_dict['type'] = location.get('type')
                    temp_dict['dimension'] = location.get('dimension')
                    temp_dict['resident_cnt'] = len(location.get('residents'))
                    locations_info.append(temp_dict)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        filtered_locations_info = [dict(t) for t in {tuple(d.items()) for d in locations_info}]
        sorted_locations_info = sorted(filtered_locations_info, key=lambda x: x['resident_cnt'], reverse=True)
        top_3_locations = sorted_locations_info[:3]
        return top_3_locations


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