from typing import Any
import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class AmarinRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns locations of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']

class AmarinRAMLocOperator(BaseOperator):
    """Top three locations of count resident on AmarinRickMortyHook"""
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Any) -> None:
        locations = []
        hook = AmarinRickMortyHook('dina_ram')
        for page in range(hook.get_char_page_count()):
            locations_temp = hook.get_char_page(page)
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
        logging.info('Top three locations:')
        logging.info(final_value)

        return final_value