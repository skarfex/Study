import requests
from pandas import json_normalize
import pandas as pd
import io
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class TopResidentsLocationsRickAndMorty(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_top_locations(self, top=3):

        location_df = []
        for n in range(1, 127):
            test = requests.get(f'https://rickandmortyapi.com/api/location/{n}')
            if test.status_code == 200:
                location_df.append(json_normalize(test.json()))
            else:
                self.result = pd.DataFrame()
        summary_df = pd.concat(location_df).reset_index(drop=True)
        summary_df['resident_cnt'] = summary_df['residents'].str.len()
        summary_df.drop(columns=['residents', 'url', 'created'], inplace=True)

        self.result = summary_df.sort_values(by='resident_cnt', ascending=False).head(top)

    def execute(self, context):

        self._get_top_locations()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # ram = io.BytesIO()
        # self.result.to_csv(ram, header=False, index=False, sep=',')
        # ram.name = f'ram.csv'
        # ram.seek(0)
        df = self.result
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = "'" + df[column] + "'"

        for _,i in self.result.iterrows():
            values = ",".join(i.to_string(header=False,index=False).split('\n'))
            sql_copy_query = f'''INSERT INTO chigishev_ram_location
VALUES ({values})
'''
            logging.info('SQL INSERT QUERY: ' + sql_copy_query)
            cursor.execute(sql_copy_query)
            conn.commit()
