from typing import List

from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
import logging

class S_halikovRamHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        return self.run('api/location').json()['info']['pages']

    def get_loc_page(self, page_num: str) -> list:
        return self.run(f'api/location?page={page_num}').json()['results']


class S_halikovRamLocationCountOperator(BaseOperator):
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.locations = []

    def get_location_count_on_page(self, result_json: List[int]):
        for loc in result_json:
            values = [loc['id'],
                      loc['name'],
                      loc['type'],
                      loc['dimension'],
                      len(loc['residents'])]
            values = list(map(str, values))
            self.locations.append(values)

    def execute(self, context):
        hook = S_halikovRamHook('dina_ram')
        for page in range(int(hook.get_loc_page_count())):
            logging.info(f'PAGE: {page + 1}')
            single_page = hook.get_loc_page(str(page + 1))
            self.get_location_count_on_page(single_page)
        self.locations.sort(key=lambda d: d[4], reverse=True)
        context['task_instance'].xcom_push(value=self.locations[:3], key='top_locations')
