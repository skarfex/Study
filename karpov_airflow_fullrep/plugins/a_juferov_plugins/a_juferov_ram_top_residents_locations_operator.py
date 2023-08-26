import requests
import logging

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class AJuferovRamLocationsHook(HttpHook):
    """
    Interacts with Rick and Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations_page_count(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_locations_on_page(self, page: int) -> list:
        """Returns count of location's residents in API"""
        return self.run(f'api/location?page={page}').json()['results']


class AJuferovRamTopResidentsLocationsOperator(BaseOperator):
    """
    Counts top n locations of Rick and Morty series by number of residents
    """

    template_fields = ('top_size',)
    ui_color = '#bada55'

    def __init__(self, top_size: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_size = top_size

    def execute(self, context) -> list:
        """
        Returns top n locations by number of residents
        """
        hook = AJuferovRamLocationsHook('dina_ram')
        locations = []
        for page in range(1, hook.get_locations_page_count() + 1):
            logging.info(f'PAGE {page}')
            page_res = hook.get_locations_on_page(page)
            for loc in page_res:
                locations.append({
                    'id': loc['id'],
                    'name': loc['name'],
                    'type': loc['type'],
                    'dimension': loc['dimension'],
                    'resident_cnt': len(loc['residents'])
                })

        return sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[:self.top_size]
