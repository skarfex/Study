
import logging
from airflow.models import BaseOperator
from p_ovchinnikov_20_plugins.p_ovchinnikov_20_api_hook import ApiHook

class ApiTopLocationOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        api_hook = ApiHook('dina_ram')
        locations = []
        for page in range(api_hook.get_count_all_pages()):
            for row in api_hook.get_list_locations(int(page) + 1):
                locations.append({
                    'id': int(row['id']),
                    'name': row['name'],
                    'type': row['type'],
                    'dimension': row['dimension'],
                    'resident_cnt': len(row['residents'])
                })

        locations = sorted(locations, key=lambda d: d['resident_cnt'], reverse=True)
        top_locations = locations[:3]
        logging.info(f'{top_locations}')

        return top_locations

