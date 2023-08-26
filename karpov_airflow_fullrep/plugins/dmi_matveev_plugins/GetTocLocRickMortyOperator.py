from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import requests
import logging


def get_count_locations(url):
    main_page = requests.get(url)
    if main_page.status_code == 200:
        logging.info('Request for the main page. Successfully')
        cnt_loc = main_page.json().get('info')['count']
        logging.info(f'Number of locations: {cnt_loc}')
        return cnt_loc
    else:
        logging.warning(f'HTTP STATUS - {main_page.status_code}')
        raise AirflowException('Error getting the main page')


def get_locations(count_loc, url):
    list_locations = []
    for x in range(1, count_loc + 1):
        url_loc = url + str(x)
        data_loc = requests.get(url_loc)
        if data_loc.status_code == 200:
            data_loc_json = data_loc.json()
            data_loc_json['residents'] = len(data_loc_json['residents'])
            list_locations.append(data_loc_json)
        else:
            logging.warning(f'HTTP STATUS - {data_loc.status_code}')
            raise AirflowException(f'Error in load from Rick&Morty API. Reference - {url_loc}')
    logging.info('The list of locations has been successfully formed Rick&Morty')
    return list_locations


class GetTocLocRickMortyOperator(BaseOperator):

    def __init__(self, top_loc=3, **kwargs):
        super().__init__(**kwargs)
        self.top_loc = top_loc

    def execute(self, context):
        url_addr = 'https://rickandmortyapi.com/api/location/'
        list_loc = get_locations(count_loc=get_count_locations(url_addr), url=url_addr)
        sort_loc = sorted(list_loc, key=lambda d: d['residents'], reverse=True)
        return sort_loc[0:self.top_loc]
