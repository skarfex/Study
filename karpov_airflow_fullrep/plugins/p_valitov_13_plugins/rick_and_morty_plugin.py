"""
Плагин для добавление топ 3 локации из сериала рик и морти для дага pvalitov_5_hw
"""

import json
import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class SearchTopLocation(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_api_data(self):

        # Get data from API
        r = requests.get('https://rickandmortyapi.com/api/location')

        # Convert it to python dictionary
        json_answer_text = json.loads(r.text)

        # Get necessary key
        self.locations = json_answer_text['results']

    def _get_top_locations(self, top=3):

        result = []  # empty list for result
        for location in self.locations:
            location_res = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            result.append(location_res)

        # Sort list of dictionaries by desc
        self.result = sorted(result, key=lambda cnt: cnt['resident_cnt'], reverse=True)[:top]

    def execute(self, context):

        # Get data from API
        self._get_api_data()

        # Get top locations
        self._get_top_locations()

        # Create connection to GreenPlum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Prepare values for inserting
        insert_values = []
        for val in self.result:
            insert_value = f"({val['id']}, '{val['name']}', '{val['type']}', '{val['dimension']}', {val['resident_cnt']})"
            insert_values.append(insert_value)

        # Save to XCOM values for insert
        sql_insert = f'''INSERT INTO "public.p_valitov_13_ram_location" VALUES {",".join(insert_values)}'''
        logging.info('SQL INSERT QUERY: ' + sql_insert)
        cursor.execute(sql_insert)
        conn.commit()
