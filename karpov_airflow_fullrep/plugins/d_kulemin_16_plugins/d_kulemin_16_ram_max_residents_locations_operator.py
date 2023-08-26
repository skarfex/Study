import requests
import logging
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


class DKulemin16NMaxResidentsLocations(BaseOperator):

    template_fields = ('n_max',)
    ui_color = '#ed7e39'

    def __init__(self, n_max: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n_max = n_max

    def get_locations_count(self, api_url):
        """
        Get count of locations in API
        :param api_url
        :return: locations count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            locations_count = r.json().get('info').get('count')
            logging.info(f'locations_count = {locations_count}')
            return locations_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load locations count')


    def get_location_series(self, result_json):
        """
        Get count of human in one page of character
        :param result_json
        :return: human count
        """
        return {
            'id': result_json.get('id'),
            'name': result_json.get('name'),
            'type': result_json.get('type'),
            'dimension': result_json.get('dimension'),
            'resident_cnt': len(result_json.get('residents', [])),
        }

    def execute(self, context):
        """
        Get N locations with max residents in Rick&Morty
        """
        locations = []
        ram_location_url = 'https://rickandmortyapi.com/api/location/'
        for location in range(self.get_locations_count(ram_location_url)):
            r = requests.get(ram_location_url + str(location + 1))
            if r.status_code == 200:
                logging.info(f'LOCATION {location + 1}')
                location_series = self.get_location_series(r.json())
                logging.info(location_series)
                locations.append(location_series)
        locations.sort(reverse=True, key=lambda x: x['resident_cnt'])
        self.locations = locations[:self.n_max]
        logging.info(f'{self.n_max} locations with max resident count in Rick&Morty are: {self.locations}')
        context['ti'].xcom_push(value=self.locations, key='d-kulemin-16-locations')
