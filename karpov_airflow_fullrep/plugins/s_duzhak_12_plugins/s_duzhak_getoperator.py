import requests
import json

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class SergDuzhGetOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        pass


    def execute(self, **kwargs):
        data = self.select_ram()
        print(data)
        return data

    def select_ram(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("cursor_date")
        cursor.execute("""
            SELECT id FROM "s-duzhak-12_ram_location";
        """)
        sql_response = cursor.fetchall()
        id_list = []
        for row in sql_response:
            id_list.append(row[0])

        response = requests.get('https://rickandmortyapi.com/api/location')
        data = json.loads(response.text)
        results = data.get('results')

        for i in range(len(results)):
            results[i]['resident_cnt'] = len(results[i].get('residents', []))

        results.sort(key=lambda x: x['resident_cnt'], reverse=True)

        transformed_data = []
        for el in results[:3]:
            transformed_data.append({
                "id": el['id'],
                "name": el['name'],
                "type": el['type'],
                "dimension": el['dimension'],
                "resident_cnt": el['resident_cnt']
            })
        return json.dumps(transformed_data)