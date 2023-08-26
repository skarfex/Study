import requests
import logging
from airflow import AirflowException
from airflow.models import BaseOperator

class KiselevRamOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context) -> None:
        url = 'https://rickandmortyapi.com/api/location/'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # создаем список словарей с нужными данными каждой локации, сортируем по убыванию количества резидентов
            locations = []
            for loc in data['results']:
                location = {
                    'id': loc['id'],
                    'name': loc['name'],
                    'type': loc['type'],
                    'dimension': loc['dimension'],
                    'residents': len(loc['residents']),
                }
                locations.append(location)
            sorted_locations = sorted(locations, key=lambda x: x['residents'], reverse=True)[:3]

            loca = []
            for sl in sorted_locations:
                my_tuple = tuple(sl.values())
                loca.append(my_tuple)

            final_value = ','.join(map(str, loca))
            return final_value
        else:
            logging.warning("HTTP STATUS {}".format(response.status_code))
            raise AirflowException('Error in load page count')