import logging
import json
import requests
from airflow.operators import BaseOperator
from airflow.exceptions import AirflowException


def get_location_count(api_url):
    """
    Get count of location in API
    :param api_url
    :return: location count
    """
    r = requests.get(api_url)
    if r.status_code == 200:
        logging.info("SUCCESS")
        location_count = r.json().get('info').get('count')
        logging.info(f'page_count = {location_count}')
        return location_count
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load location count')


def get_residents_count_on_location(result_json):
    """
    Get count of resident in one location
    :param result_json
    :return: resident count
    """
    residents_count_on_location = len(result_json.json().get('residents'))

    logging.info(f'residents_count_on_location = {residents_count_on_location}')
    return residents_count_on_location


def top3(kwargs):
    max = sorted(kwargs, key=kwargs.get)[-3:]
    b = {max[2]: kwargs[max[2]], max[1]: kwargs[max[1]], max[0]: kwargs[max[0]]}
    answer = b.keys()
    return answer


def load_ram_func():
    """
    Logging dict locations and count residents in Rick&Morty
    """
    location = {}
    locations = []
    url = "https://rickandmortyapi.com/api/location/"
    ram_char_url = 'https://rickandmortyapi.com/api/location/{pg}'
    for page in range(get_location_count(url)):
        r = requests.get(ram_char_url.format(pg=str(page + 1)))
        if r.status_code == 200:
            j = json.loads(r.text)
            a = get_residents_count_on_location(r)
            logging.info(f'id location {location} : count residents {a}')
            location[j['id']] = a
            l = {}
            for key in j:
                if key in ("id", "name", "type", "dimension"):
                    l[key] = j[key]
            l["resident_cnt"] = a
            locations.append(l)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')

    logging.info(f'TOP3  : {top3(location)}')
    top = top3(location)
    answer = []
    for a in locations:
        if a["id"] in top:
            answer.append(a)
    return answer


class PirogovTop3LocationOperator(BaseOperator):
    """
    Top3 Location Rick_and_Morty
    """
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.answer = []

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        location = {}
        locations = []
        url = "https://rickandmortyapi.com/api/location/"
        ram_char_url = 'https://rickandmortyapi.com/api/location/{pg}'
        for page in range(get_location_count(url)):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                j = json.loads(r.text)
                a = get_residents_count_on_location(r)
                logging.info(f'id location {location} : count residents {a}')
                location[j['id']] = a
                l = {}
                for key in j:
                    if key in ("id", "name", "type", "dimension"):
                        l[key] = j[key]
                l["resident_cnt"] = a
                locations.append(l)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        logging.info(f'TOP3  : {top3(location)}')
        top = top3(location)
        answer = []
        for a in locations:
            if a["id"] in top:
                answer.append(a)
        logging.info(f'TOP3  : {answer}')
        self.answer = answer
