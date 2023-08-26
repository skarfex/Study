import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class eponomarevaRamSpeciesCountOperator(BaseOperator):
    """
    Count number of dead concrete species
    """
    template_fields = ('species_type',) #определяет, какие поля могут быть шаблонизированы
    ui_color = "#e0ffff"

    def __init__(self, species_type: str = 'Human', **kwargs) -> None:      #по умолчанию Human + список именованных переменных
        super().__init__(**kwargs)                                          #метод суперкласса
        self.species_type = species_type

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        species_count = 0
        ram_char_url = 'https://rickandmortyapi.com/api/character/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                species_count += self.get_human_count_on_page(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        logging.info(f'{self.species_type} in Rick&Morty: {species_count}')






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

