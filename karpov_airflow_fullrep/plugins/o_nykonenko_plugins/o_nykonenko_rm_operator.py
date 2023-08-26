from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
import logging

class ONykonenkoRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']



class ONykonenkoRickMortyOperator(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        hook = ONykonenkoRickMortyHook('dina_ram')
        locations = []
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            for loc in one_page:
                location = {'id': loc['id']}
                location['name'] = loc['name']
                location['type'] = loc['type']
                location['dimension'] = loc['dimension']
                location['resident_cnt'] = len(loc['residents'])
                locations.append(location)
        locations = sorted(locations, key=lambda d: d['resident_cnt'], reverse=True)
        top_locations = locations[:3]
        logging.info(f'{top_locations}')
        return top_locations
