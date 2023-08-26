import requests

import pandas as pd
import logging
# def get_pages_number()
from airflow.models import BaseOperator

url = 'https://rickandmortyapi.com/documentation/#location'
row = requests.get(url)
row

class Kasatkin_ram_toploc(BaseOperator):
    '''

    '''
    def __init__(self, tops=3, **kwrgs) -> None:
        super().__init__( **kwrgs)
        self.tops = tops

    def pages_numb(self, url):
        row = requests.get(url)
        pages = row.json().get('info').get('pages')
        return pages

    def get_location_data(self, result):
        res_df = pd.DataFrame()
        for row in result.json().get('results'):
            row_df = pd.DataFrame.from_dict(row)
            res_df = pd.concat([res_df, row_df], ignore_index=True)
        return res_df

    def execute(self, context):
        df = pd.DataFrame()
        url = 'https://rickandmortyapi.com/api/location'
        for page in range(self.pages_numb(url)):
            logging.info(f'page_numb - {page + 1}')
            data = requests.get(f"{url}?page={str(page + 1)}")
            df_append = self.get_location_data(data)
            df_append = df_append.groupby(['id', 'name', 'type', 'dimension'], as_index=False).agg({'residents': 'count'})
            df = pd.concat([df, df_append], ignore_index=True)
        df.sort_values('residents', ascending=False, inplace=True)
        values = list(df.itertuples(index=False, name=None))[:self.tops]
        values_to_load = ','.join(map(str, values))
        logging.info(f"Done and ready with {values_to_load}")
        return values_to_load

