import csv
import logging
from typing import Any

import requests
from airflow import AirflowException
from airflow.models import BaseOperator


def get_page_count(api_url):
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


def get_human_count_on_page(result_json):
    """
    Get count of human in one page of character
    :param result_json
    :return: human count
    """
    human_count_on_page = 0
    for one_char in result_json:
        if one_char.get('species') == 'Human':
            human_count_on_page += 1
    logging.info(f'human_count_on_page = {human_count_on_page}')
    return human_count_on_page


def load_ram_func():
    """
    Logging count of Human in Rick&Morty
    """
    human_count = 0
    ram_char_url = 'https://rickandmortyapi.com/api/character/?page={pg}'
    for page in range(get_page_count(ram_char_url.format(pg='1'))):
        r = requests.get(ram_char_url.format(pg=str(page + 1)))
        if r.status_code == 200:
            logging.info(f'PAGE {page + 1}')
            human_count += get_human_count_on_page(r.json().get('results'))
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')
    logging.info(f'Humans in Rick&Morty: {human_count}')


def get_top_residents_locations():
    result = []

    # url for all locations
    locations_url = "https://rickandmortyapi.com/api/location"

    content = requests.get(locations_url)
    if content.status_code == 200:
        logging.info("Location content recieved!")
        for location in content.json().get('results'):
            location_stats = {
                "id": location["id"],
                "name": location["name"],
                "type": location['type'],
                "dimension": location['dimension'],
                "resident_cnt": len(location['residents']),
            }
            result.append(location_stats)

        # Sort by residents number
        result = sorted(result, key=lambda x: x['resident_cnt'], reverse=True)
        print(result)
    else:
        logging.warning("HTTP STATUS {}".format(content.status_code))
        raise AirflowException('Error in load page count')


class VerkhovtsevRAMLocations(BaseOperator):
    """
    Count top-k locations from Rick and Morty series.
    """
    ui_color = "#e0ffff"

    def __init__(self, top_k: int = 3, **kwargs):
        """

        :param top_k: how many positions need to return (1 - highest, 3 - top 3 etc)
        :param kwargs:
        """
        super().__init__(**kwargs)
        self.top_k = top_k

    def execute(self, context: Any):
        result = []

        # url for all locations
        locations_url = "https://rickandmortyapi.com/api/location"

        content = requests.get(locations_url)
        if content.status_code == 200:
            logging.info("Location content recieved!")
            for location in content.json().get('results'):
                location_stats = {
                    "id": location["id"],
                    "name": location["name"],
                    "type": location['type'],
                    "dimension": location['dimension'],
                    "resident_cnt": len(location['residents']),
                }
                result.append(location_stats)

            # Sort by residents number
            result = sorted(result, key=lambda x: x['resident_cnt'], reverse=True)
            result = result[:self.top_k] if len(result) >= self.top_k else result
            context['ti'].xcom_push(key=f"verkhovtsev_ram_top{self.top_k}", value=result)
            logging.info(f"Result of top_{self.top_k}: {result}")
            return result

        else:
            logging.warning("HTTP STATUS {}".format(content.status_code))
            raise AirflowException('Error in load page count')



if __name__ == "__main__":
    # load_ram_func()
    get_top_residents_locations()