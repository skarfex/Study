# -*- coding: utf-8 -*-

import logging
from airflow.models import BaseOperator
from o_romanishen_2_plugins.o_romanishen_2_api_hook import ApiHook


class ApiTopPageOperator(BaseOperator):

    def __init__(self, page_number: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.page_number = page_number

    def _change_residents_to_count(self, locations: list) -> None:
        for location in locations:
            location['residents'] = len(location['residents'])

    def _get_top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents'], reverse=True)
        return sorted_locations[:self.page_number]

    def _get_sql_values(self, locations: list) -> str:
        sql_values = ''
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})"
        return sql_values

    def execute(self, context):

        hook_cnt = ApiHook('dina_ram')
        pages_count = hook_cnt.get_cnt_all_pages()

        locations = []
        for page_id in range(1, pages_count + 1):
            page_locations = hook_cnt.get_page(page_id)
            self._change_residents_to_count(page_locations)
            locations.extend(page_locations)

        top_locations = self._get_top_locations(locations)
        logging.info(f'Where are {len(top_locations)} top locations')

        return self._get_sql_values(top_locations)