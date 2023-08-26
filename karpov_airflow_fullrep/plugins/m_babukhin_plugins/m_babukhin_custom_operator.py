"""
Custom operator for loading data via Rick and Morty API about locations
"""
import logging
import csv
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BabukhinLocationDiscoveryOperator(BaseOperator):
    """
    Operator, that count residents on each location via RickandMorty API and returns top-3 locations by residents count
    """
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        url = 'https://rickandmortyapi.com/api/location'
        json_file = requests.get(url).json()
        locations_number = json_file['info']['count']
        result_dict = {}
        sorted_top = []

        for i in range(1, locations_number+1):
            residents_number = len(requests.get(
                url + f'/{i}').json()['residents'])
            if len(result_dict) < 3:
                result_dict[i] = residents_number
            else:
                min_value = sorted_top[-1]
                if residents_number <= min_value:
                    continue
                else:
                    for key, value in result_dict.items():
                        if value == min_value:
                            del result_dict[key]
                            break
                    result_dict[i] = residents_number
            sorted_top = sorted(result_dict.values(), reverse=True)

        with open('/tmp/location_data.csv', 'w', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for key, value in result_dict.items():
                page = requests.get(url + f'/{key}').json()
                writer.writerow([page['id']] + [page['name']] + [page['type']] +
                                [page['dimension']] + [len(page['residents'])])
                logging.info([page['id']] + [page['name']] + [page['type']] +
                             [page['dimension']] + [len(page['residents'])])
