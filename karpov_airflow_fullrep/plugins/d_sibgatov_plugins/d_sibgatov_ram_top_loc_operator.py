from airflow.models import BaseOperator
from d_sibgatov_plugins.d_sibgatov_ram_hook import DSibgatovRAMHook
import logging

class DSibgatovRAMOperator(BaseOperator):

    def __init__(self, count_top_locations: int = 3, **kwargs)->None:
        super().__init__(**kwargs)
        self.count_top_locations = count_top_locations


    def get_resident_count(self, locations: list) -> None:
        for location in locations:
            location['residents'] = len(location['residents'])

    def top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents'], reverse=True)
        return sorted_locations[:self.count_top_locations]

    def _get_sql_values(self, locations: list) -> str:
        sql_values = ''
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})"
        return sql_values

    def execute(self, context):
        hook = DSibgatovRAMHook('dina_ram')
        locations = []
        for page in range(1, hook.get_location_pages_count()+1):
            logging.info(f'PAGE{page}')
            one_page = hook.get_page_location(str(page))
            self.get_resident_count(one_page)
            locations.extend(one_page)

        top_locations = self.top_locations(locations)
        logging.info(f'Where are {len(top_locations)} top locations')
        return self._get_sql_values(top_locations)





