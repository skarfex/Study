import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class DTeplovaRamLocationOperator(BaseOperator):
    """
    Top-3 locations with info on residents
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: locations count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            loc_count = r.json().get('info').get('pages')
            logging.info(f'loc_count = {loc_count}')
            return int(loc_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_loc_info(self, result_json: list, location: list) -> list:
        """
        Get location info
        :param result_json:
        :return: location info
        """
        for one_loc in result_json:
            location.append(
                [
                    one_loc.get("id"),
                    one_loc.get("name"),
                    one_loc.get("type"),
                    one_loc.get("dimension"),
                    len(one_loc.get("residents"))
                ])
            return location

    def execute(self, context):
        """
        Logging count of concrete locations in Rick&Morty
        """
        location = []
        ram_char_url = "https://rickandmortyapi.com/api/location?page={pg}"

        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                location = self.get_loc_info(r.json().get('results'), location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        top_loc = pd.DataFrame(
            data=location,
            columns=['id', 'name', 'type', 'dimension', 'resident_cnt']
        ).sort_values(by='resident_cnt', ascending=False).head(3)

        return top_loc
