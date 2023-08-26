
import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class GHaritonovaRamLocationOperator(BaseOperator):
    """
    Top 3 locations by number of residents
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_next_page(self, api_url: str) -> str:
        """
        Get next page in API
        :param api_url
        :return: api_url
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            next_page = r.json().get('info').get('next')
            logging.info(f'next_page = {next_page}')
            return next_page
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_results(self, api_url: str) -> dict:
        """
        Get results from page
        :param api_url
        :return: dict
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info(f'successfully read page = {api_url}')
            results = r.json().get('results')
            return results
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        location_df = pd.DataFrame(
            columns=['id', 'name', 'type', 'dimension', 'resident_cnt']
        )
        ram_loc_url = 'https://rickandmortyapi.com/api/location'
        while ram_loc_url:
            results = self.get_results(ram_loc_url)
            for location in results:
                resident_cnt = location.get('residents').__len__()
                location_df.loc[len(location_df) + 1] = [
                    location.get('id'),
                    location.get('name'),
                    location.get('type'),
                    location.get('dimension'),
                    location.get('residents').__len__()
                ]
            ram_loc_url = self.get_next_page(ram_loc_url)
        location_df = location_df.sort_values(by = 'resident_cnt', ascending=False).head(3)
        print(location_df.head(5))
        location_df.to_csv('/tmp/g-haritonova-locations.csv', index=False, header=False)

