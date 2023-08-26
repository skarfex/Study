from airflow.models import BaseOperator
from n_serikbaev_17_plugins.n_serikbaev_17_api_hook import n_serikbaev_17_api_hook
import logging


class NSerikbaev17RaMLocationOperator(BaseOperator):
    def __init__(self, special_number: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.special_number = special_number

    
    def generate_sql_values(self, top_locations: list) -> str:
        sql_values = ''
        for location in top_locations:
            sql_values += '' if not sql_values else ','
            sql_values += f"({location[0]}, '{location[1]}', '{location[2]}', '{location[3]}', {location[4]})"
        return sql_values
    
    
    def execute(self, context):
        hook = n_serikbaev_17_api_hook('dina_ram')

        locations = []

        for page in range(hook.get_location_page_count()):
            logging.info(f'page {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            for location in one_page:
                locations.append([location['id'], location['name'], location['type'],
                    location['dimension'], len(location['residents'])])
        
        logging.info(f'locations are {locations}')

        top_locations = sorted(locations, key=(lambda l: l[4]), reverse=True)[:self.special_number]

        logging.info(f'top_locations are {top_locations}')

        sql_values = self.generate_sql_values(top_locations)

        logging.info(f'SQL values are: {sql_values}')

        return sql_values