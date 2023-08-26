import logging
from airflow.models import BaseOperator
from a_gunicheva_16_plugins.a_gunicheva_16_ApiHook import GunichevaApiHook

class GunichevaTopLocationsOperator(BaseOperator):
    def __init__(self, top_number: int=3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_number = top_number

    def _add_residents_count(self, locations: list) -> None:
        for location in locations:
            location['residents_cnt'] = len(location['residents'])

    def _get_top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents_cnt'], reverse=True)
        return sorted_locations[:self.top_number]

    def _get_sql_values(self, locations: list) -> str:
        sql_values = ''
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents_cnt']})"
        return sql_values

    def execute(self, context):

        api_hook = GunichevaApiHook('dina_ram')
        pages_count = api_hook.get_location_page_count()

        locations = []
        for page_num in range(1, pages_count + 1):
            page_locations = api_hook.get_char_page(page_num)
            self._add_residents_count(page_locations)
            locations.extend(page_locations)
        
        logging.info(f'locations is {locations}')

        top_number = sorted(self._get_top_locations(locations), key=lambda d: d['residents_cnt'], reverse=False)

        logging.info(f'top_locations is {top_number}')

        return self._get_sql_values(top_number)
