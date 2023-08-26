from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
import logging


class IRomanchevRickAndMortyHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']


class IRomanchevRickAndMortyOperator(BaseOperator):

    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_location_info_on_page(self, result_json: list) -> list:
        locations = []
        for one_location in result_json:
            residents_count = len(one_location.get('residents'))
            location_id = one_location.get('id')
            location_name = one_location.get('name')
            location_type = one_location.get('type')
            location_dimension = one_location.get('dimension')
            locations.append([residents_count, location_id, location_name, location_type, location_dimension])
        return locations

    def execute(self, context, **kwargs):
        hook = IRomanchevRickAndMortyHook('rick_and_morty')
        locations = []
        for page in range(hook.get_loc_page_count()):
            one_page = hook.get_location_page(str(page + 1))
            locations += self.get_location_info_on_page(one_page)
        locations_sorted = sorted(locations, key=lambda x: x[0], reverse=True)
        logging.info(f'First 3 locations in Rick&Morty are {locations_sorted[0:3]}')
        return locations_sorted[0:3]
        #kwargs['ti'].xcom_push(value=locations_sorted[0:3], key='locations')
