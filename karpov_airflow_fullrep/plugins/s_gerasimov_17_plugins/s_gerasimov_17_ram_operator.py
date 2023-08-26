from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
import logging


class SGerasimovTop3LocationHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_locations_page(self, page_number: int) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_number}').json()['results']


class SGerasimovTop3LocationOperator(BaseOperator):

    def __init__(self, top_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_count = top_count

    def execute(self, context):
        hook = SGerasimovTop3LocationHook('dina_ram')
        locations = []
        for page_number in range(1, hook.get_locations_page_count()+1):
            locations_on_page = hook.get_locations_page(page_number)
            locations.extend(
                [dict({'id': loc['id'], 'name': loc['name'], 'type': loc['type'], 'dimension': loc['dimension'], 'resident_cnt': len(loc['residents'])})
                              for loc in locations_on_page]
            )
        locations.sort(reverse=True, key=lambda x: x['resident_cnt'])
        locations = locations[:self.top_count]
        context['ram_top_locations'] = locations
        logging.info(f'Top {self.top_count} locations in Rick&Morty:')
        for loc in locations:
            logging.info(loc)
        return locations


