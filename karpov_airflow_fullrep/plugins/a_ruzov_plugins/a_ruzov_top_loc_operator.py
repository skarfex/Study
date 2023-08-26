""" Hook to Rick & Morty API
OUTPUT: xcom
"""
import logging
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator


class ARuzovRamHook(HttpHook):

    def __init__(self, http_conn_id: str, method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_location_pages_count(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_page_locations(self, page_id) -> list:
        return self.run(f'api/location?page={page_id}').json()['results']


class ARuzovGetTopRamLocationsOperator(BaseOperator):

    def __init__(self, locations_number: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.locations_number = locations_number

    def _change_residents_to_count(self, locations: list) -> None:
        for location in locations:
            location['residents'] = len(location['residents'])

    def _get_top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents'], reverse=True)
        return sorted_locations[:self.locations_number]

    def _get_sql_values(self, locations: list) -> str:
        sql_values = ''
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})"
        return sql_values

    def execute(self, context):

        ram_hook = ARuzovRamHook('dina_ram')
        pages_count = ram_hook.get_location_pages_count()

        locations = []
        for page_id in range(1, pages_count + 1):
            page_locations = ram_hook.get_page_locations(page_id)
            self._change_residents_to_count(page_locations)
            locations.extend(page_locations)

        top_locations = self._get_top_locations(locations)
        logging.info(f'Where are {len(top_locations)} top locations')

        return self._get_sql_values(top_locations)