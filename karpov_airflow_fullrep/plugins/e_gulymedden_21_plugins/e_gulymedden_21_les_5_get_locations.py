import logging
from typing import List

import psycopg2
import requests
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class Les_5_get_locations(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_locations(self, api_url: str) -> List:
        r = requests.get(url=api_url)
        if r.status_code == 200:
            data = r.json().get('results')
            dict_results = {}
            length_of_results = len(data)
            for i in range(length_of_results):
                loc = data[i]
                id = loc.get("id")
                res_count = loc.get("residents")
                dict_results[id] = len(res_count)

            logging.info(f'All id with count of residents : \n{dict_results}')
            max_top_3_locations = sorted(dict_results, key=dict_results.get, reverse=True)[:3]
            logging.info(f'Maximum top 3 locations: {max_top_3_locations}')
            return max_top_3_locations
        else:
            logging.warning(f'HTTP ERROR STATUS: {r.status_code}')
            raise AirflowException('Error: Can not read api_url correctly!')

    def execute(self, context):
        api_url = 'https://rickandmortyapi.com/api/location'
        loc_id = self.get_locations(api_url)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        query = "TRUNCATE TABLE e_gulymedden_21_ram_location"
        pg_hook.run(query, False)
        for i in loc_id:
            url_loc = f'https://rickandmortyapi.com/api/location/{i}'
            loc_info = requests.get(url=url_loc).json()
            query = f'''
            INSERT INTO e_gulymedden_21_ram_location
            (id, name, type, dimension, resident_cnt)
            VALUES (
                    {loc_info.get('id')}, '{loc_info.get('name')}',
                   '{loc_info.get('type')}', '{loc_info.get('dimension')}',
                    {len(loc_info.get('residents'))}
                    )
                    '''
            logging.info(f'{query}')
            try:
                pg_hook.run(query, False)
            except (psycopg2.Error) as error:
                logging.info(f"Errors during insert {error}")
