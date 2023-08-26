import logging
import requests
import json

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class FNiyazovaRamTop3LocationOperator(BaseOperator):
    '''
    Count residents' number of each location
    and logging 3 locations with max count number of residents
    '''
    ui_color = '#C2F2F0'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_residents_count(self, *args, **kwargs):
        url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(url)
        if r.status_code == 200:
            locations = json.loads(r.text) 
            locations_results = locations['results']
            res = []
            for loc in locations_results:
                temp_res = {
                    'id': loc['id'],
                    'name': loc['name'],
                    'type': loc['type'],
                    'dimension': loc['dimension'],
                    'resident_cnt': len(loc['residents'])
                }
                res.append(temp_res)
        else:
            logging.warning(f'Error{r.status_code}')
        
        sorted_res = sorted(res, key = lambda x: x['resident_cnt'], reverse=True)
        return sorted_res[:3]


    def execute(self, context, *args, **kwargs):
        return self.get_residents_count()
        
