from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd

class VPavlovskijRAMHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_page_count(self):
        return int(self.run('api/location').json()['info']['pages']) + 1

    def get_results(self, page_num):
        return self.run(f'api/location?page={page_num}').json()['results']

class VPavlovskijLocationOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        hook = VPavlovskijRAMHook('dina_ram')
        df = []
        for  i in range(1, hook.get_page_count()):
            results = hook.get_results(i)
            for location in results:
                row = {}
                row['id'] = location['id']
                row['name'] = location['name']
                row['type'] = location['type']
                row['dimension'] = location['dimension']
                row['resident_cnt'] = len(location['residents'])
                df.append(row)
        context['ti'].xcom_push(value=pd.DataFrame(df).sort_values('resident_cnt', ascending=False).iloc[:3], key='table')
