import csv
import requests
import logging
from http import HTTPStatus
from collections import namedtuple

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


class NkalimanovLocationsTopOperator(BaseOperator):
    """
    Find three locations of the series "Rick and Morty" with most residents
    and upload data to GreenPlum.
    """

    ui_color = "#e0ffff"

    def __init__(self, top_locations_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_locations_count = top_locations_count

    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        response = requests.get(api_url)

        if response.status_code == HTTPStatus.OK:
            logging.info("SUCCESS")
            page_count = response.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(response.status_code))
            raise AirflowException('Error in load page count')

    def get_locations_residents_count_on_page(self, result_json):
        """
        Get count of residents in one page of location
        :param result_json
        :return: residents count
        """
        locations_residents_cnt = []
        Location = namedtuple('Location',[
            'id',
            'name',
            'type',
            'dimension',
            'resident_cnt',
        ])
        for one_char in result_json:
            locations_residents_cnt.append(Location(
                one_char.get('id'),
                one_char.get('name'),
                one_char.get('type'),
                one_char.get('dimension'),
                len(one_char.get('residents')),
            ))
        logging.info(f'locations_residents_cnt = {locations_residents_cnt}')
        return locations_residents_cnt

    def data_to_csv(self, data):
        """
        Load data to csv.
        """
        with open('/tmp/n-kalimanov_rick_morty.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerows(data)

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty.
        """
        locations_residents_cnt_sort = []
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            response = requests.get(ram_char_url.format(pg=str(page + 1)))
            if response.status_code == HTTPStatus.OK:
                logging.info(f'PAGE {page + 1}')
                locations_residents_cnt = self.get_locations_residents_count_on_page(response.json().get('results'))
                locations_residents_cnt_sort.extend(locations_residents_cnt)
            else:
                logging.warning("HTTP STATUS {}".format(response.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        locations_residents_cnt_sort.sort(key=lambda x: x.resident_cnt, reverse=True)
        logging.info(f'Top most residents in locations Rick&Morty: {locations_residents_cnt_sort[:self.top_locations_count]}')
        self.data_to_csv(locations_residents_cnt_sort[:self.top_locations_count])
        return '/tmp/n-kalimanov_rick_morty.csv'
