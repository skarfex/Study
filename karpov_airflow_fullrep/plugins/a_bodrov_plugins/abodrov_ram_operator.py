import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class ABodrovRickMortyAPIHook(HttpHook):
    """
    Interaction with Rick and Morty API (https://rickandmortyapi.com/api/)
    """

    def __init__(self, http_conn_id, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self):
        """ Returns count of pages in https://rickandmortyapi.com/api/location"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num):
        """ Returns locations"""
        return self.run('api/location?page={page_num}'.format(page_num=page_num+1)).json()['results']


class ABRRickMortyAPILocationsOperator(BaseOperator):

    ui_color = "#c7ffe9"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_locations(self):
        locations = []
        hook = ABodrovRickMortyAPIHook('dina_ram')
        for page in range(hook.get_location_page_count()):
            locations_raw = hook.get_location_page(page)
            for location in locations_raw:
                locations.append({
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents'])
                })
        return locations

    def get_top_locations(self, locations):
        df = pd.DataFrame(locations)
        top_locations = df.sort_values(by=['resident_cnt'], ascending=False).head(3)
        return top_locations.to_json(orient='values')

    def execute(self, context):
        locations = self.get_locations()
        top_locations = self.get_top_locations(locations)
        return top_locations
