import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class IvanRamTopLocationsCountResidents(BaseOperator):
    """
    Select top N locations with the most amount of residents
    """

    template_fields = ('top_locations',)
    ui_color = '#e0ffff'

    def __init__(self, top_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_locations = top_locations

    def rick_morty_cnt_locations(self, api_url: str) -> int:
        """
        Get count of locations in API
        :param api_url
        :return: locations count
        """
        response = requests.get(api_url)
        if response.status_code == 200:
            logging.info('SUCCESS')
            locations_count = response.json().get('info').get('count')
            logging.info(f'locations_count = {locations_count}')
            return int(locations_count)
        else:
            logging.warning(f'HTTP STATUS {response.status_code}')
            raise AirflowException('Error in load locations count')

    def cnt_residents_at_location_info(self, location_json: list) -> tuple:
        """
        Get count of residents at concrete location
        :param location_json
        :return: id, name, type, dimension, residents count
        """
        return (location_json.get('id'),
                location_json.get('name'),
                location_json.get('type'),
                location_json.get('dimension'),
                len(location_json.get('residents')))

    def execute(self, context):
        """
        Return list of tuples top N locations this the most amount of residents
        """
        api_url = 'https://rickandmortyapi.com/api/location'
        array = []
        for i in range(1, self.rick_morty_cnt_locations(api_url) + 1):
            response = requests.get(f'https://rickandmortyapi.com/api/location/{i}')
            if response.status_code == 200:
                array.append(self.cnt_residents_at_location_info(response.json()))
            else:
                logging.warning(f'HTTP status_code: {response.status_code}')
                # raise AirflowException('Error in load from API')

        sorted_top = sorted(array, key=lambda x: x[4], reverse=True)[:self.top_locations]
        return sorted_top
        # logging.info(sorted_top)

