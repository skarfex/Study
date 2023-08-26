from airflow.models.baseoperator import BaseOperator
from n_chepurnyh_19_plugins.n_chepurnyh_19_ram_hook import NChepurnyh19RickMortyHook
import requests
import logging


class NChepurnyh19RamLocationOperator(BaseOperator):
    def __init__(self, count_top_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.count_top_locations = count_top_locations

    def get_resident_count(self, locations: list) -> None:
        for location in locations:
            location['residents'] = len(location['residents'])

    def top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents'], reverse=True)
        return sorted_locations[:self.count_top_locations]

    def get_sql_values(self, locations: list) -> str:
        sql_values = ''
        sql_val = []
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})"
            result = []
            result.append(row['id'])
            result.append(row['name'])
            result.append(row['type'])
            result.append(row['dimension'])
            result.append(row['residents'])
            sql_val.append(result)
            logging.info(f' sql_val : {result}')
        logging.info(f' sql_val : {sql_val}')
        return sql_val

    def execute(self, context):
        hook = NChepurnyh19RickMortyHook('dina_ram')
        locations = []
        for page in range(1, hook.get_location_pages_count()+1):
            logging.info(f'PAGE{page}')
            one_page = hook.get_page_location(str(page))
            self.get_resident_count(one_page)
            locations.extend(one_page)

        top_locations = self.top_locations(locations)
        logging.info(f'Where are {len(top_locations)} top locations')
        logging.info(f'Where are {self.get_sql_values(top_locations)} top locations')
        logging.info(f' Answer {self.get_sql_values(top_locations)}')
        logging.info(f' Answer')
        context['ti'].xcom_push(value=self.get_sql_values(top_locations), key='n_chepurnyh_19_locations')


"""
классы с обучения
"""


class NChepurnyh19RamSpeciesCountOperator(BaseOperator):
    """
    Count number of dead concrete species
    """
    template_fields = ('species_type',)
    ui_color = "#e0ffff"

    def __init__(self, species_type: str = 'Human', **kwargs) -> None:
        super().__init__(**kwargs)
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


class NChepurnyh19RamDeadOrAliveCountOperator(BaseOperator):
    """
    Count number of dead or alive characters
    on NChepurnyh19RickMortyHook
    """

    template_fields = ('dead_or_alive',)
    ui_color = "#c7ffe9"

    def __init__(self, dead_or_alive: str = 'Dead', **kwargs) -> None:
        super().__init__(**kwargs)
        self.dead_or_alive = dead_or_alive

    def get_dead_or_alive_count_on_page(self, result_json: list) -> int:
        """
        Get count of dead or alive in one page of character
        :param result_json
        :return: dead_or_alive_count
        """
        dead_or_alive_count_on_page = 0
        for one_char in result_json:
            if one_char.get('status') == self.dead_or_alive:
                dead_or_alive_count_on_page += 1
        logging.info(f'{self.dead_or_alive} count_on_page = {dead_or_alive_count_on_page}')
        return dead_or_alive_count_on_page

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = NChepurnyh19RickMortyHook('dina_ram')
        dead_or_alive_count = 0
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            dead_or_alive_count += self.get_dead_or_alive_count_on_page(one_page)
        logging.info(f'{self.dead_or_alive} in Rick&Morty: {dead_or_alive_count}')
