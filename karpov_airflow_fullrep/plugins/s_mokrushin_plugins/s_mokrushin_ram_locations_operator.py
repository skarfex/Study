from typing import Any
import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook

class SmokrushinRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in https://rickandmortyapi.com/api/location"""
        return self.run('api/location').json()['info']['pages']

    def get_loc_page(self, page_num: str) -> list:
        """Returns locations"""
        return self.run(f'api/location?page={page_num}').json()['results']

class SmokrushinRAMAPILocationsOperator(BaseOperator):
    """Top3 locations of resident_cnt on SmokrushinRickMortyHook"""
    ui_color = "#aed993"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: Any) -> None:
        locations = []
        hook = SmokrushinRickMortyHook('dina_ram')
        for page in range(hook.get_loc_page_count()):
            locations_temp = hook.get_loc_page(page)
            for location in locations_temp:
                row = (
                        location['id'],
                        location['name'],
                        location['type'],
                        location['dimension'],
                        len(location['residents'])
                )
                locations.append(row)
        top_locations = sorted(list(set(locations)), key=lambda x: x[-1], reverse=True)[:3]
        final_value = ','.join(map(str, top_locations))
        logging.info('Top 3 locations:')
        logging.info(final_value)

        return final_value