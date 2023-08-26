from airflow.models import BaseOperator
from p_pirogov_plugins.p_pirogov_ram_hook import PirogovHook
import logging

class PirogovOperator(BaseOperator):

    def __init__(self, count_top_locations: int = 3, **kwargs)->None:
        super().__init__(**kwargs)
        self.count_top_locations = count_top_locations


    def get_resident_count(self, locations: list) -> None:
        for location in locations:
            location['residents'] = len(location['residents'])

    def top_locations(self, locations: list) -> list:
        sorted_locations = sorted(locations, key=lambda d: d['residents'], reverse=True)
        return sorted_locations[:self.count_top_locations]

    def get_sql_values(self, locations: list) -> str:
        sql_values = ''
        sql_val = []
        for row in locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({row['id']}, '{row['name']}', '{row['type']}', '{row['dimension']}', {row['residents']})"
            result = []
            result.append(row['id'])
            result.append(row['name'])
            result.append(row['type'])
            result.append(row['dimension'])
            result.append(row['residents'])
            sql_val.append(result)
            logging.info(f' sql_val : {result}')
        logging.info(f' sql_val : {sql_val}')
        return sql_val


    def execute(self, context):
        hook = PirogovHook('dina_ram')
        locations = []
        for page in range(1, hook.get_location_pages_count()+1):
            logging.info(f'PAGE{page}')
            one_page = hook.get_page_location(str(page))
            self.get_resident_count(one_page)
            logging.info(f'self.get_resident_count(one_page) {one_page}')
            locations.extend(one_page)

        logging.info(f'locations : {locations}')
        top_locations = self.top_locations(locations)
        logging.info(f'Where are {len(top_locations)} top locations')
        logging.info(f'Where are {self.get_sql_values(top_locations)} top locations')
        logging.info(f' Answer {self.get_sql_values(top_locations)}')
        logging.info(f' Answer')
        context['ti'].xcom_push(value=self.get_sql_values(top_locations), key='p_pirogov_locations')






