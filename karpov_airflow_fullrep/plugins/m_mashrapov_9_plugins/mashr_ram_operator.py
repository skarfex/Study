import pandas
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class MashrRamHttpHook(HttpHook):
    """Hook for Rick and Morty API"""

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self):
        """ Returns count of pages in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num):
        """ Returns locations"""
        return self.run(f'api/location?page={page_num}').json()['results']        

class MashrRamOperator(BaseOperator):
    """Operator Get TOP-3 Rick and Morty Locations"""

    ui_color='#1ABB9B'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        hook = MashrRamHttpHook('dina_ram')
        ram_locations = []
        for page in range(hook.get_location_page_count()):
            locations = hook.get_location_page(page + 1)
            for loc in locations:
                ram_locations.append({
                    'id': loc['id'],
                    'name': loc['name'],
                    'type': loc['type'],
                    'dimension': loc['dimension'],
                    'resident_cnt': len(loc['residents'])
                })
        data_frame = pandas.DataFrame(ram_locations)
        top_3_loc = data_frame.sort_values(by=['resident_cnt'], ascending=False).head(3)
        return top_3_loc.to_json(orient='records')