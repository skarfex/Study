from airflow.models.baseoperator import BaseOperator
# from airflow.hooks.postgres_hook import PostgresHook

import requests
import pandas as pd
import logging
from airflow.exceptions import AirflowException


class GrebenkinRamLocationsOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_page_quantity(self):
        """
        This function gets quantity of pages from R&M API
        """
        url_ram_pages = f'https://rickandmortyapi.com/api/location'
        response = requests.get(url_ram_pages)
        if response.status_code == 200:
            json_ram_pages = response.json()
            page_quantity = json_ram_pages['info']['pages']
            return page_quantity
        else:
            logging.warning(f'HTTP code is {response.status_code}')
            raise AirflowException('Error! Can`t get API')

    def execute(self, context):
        """
        This function gets location info from R&M API;
        for every location in page it counts residents;
        calculates top three locations by resident count;
        saves result dataframe into .csv
        """
        results = []
        for page in range(1, self.get_page_quantity()):
            url_ram_info = f'https://rickandmortyapi.com/api/location/?page={page}'
            response = requests.get(url_ram_info)
            if response.status_code == 200:
                json_ram_info = response.json()
                page_results = json_ram_info['results']
                for location in range(len(page_results)):
                    page_result_location = page_results[location]
                    resident_count = {'resident_count': len(page_result_location['residents'])}
                    page_result_location.update(resident_count)
                    results.append(page_result_location)
            else:
                logging.warning(f'HTTP code is {response.status_code}')
                raise AirflowException('Error! Can`t get pages')
        df = pd.DataFrame(results)
        df = df.loc[:, ~df.columns.isin(['residents', 'url', 'created'])]
        top_three_locations = df.sort_values(by='resident_count', ascending=False).head(3)
        file_name = 'Grebenkin_ram.csv'
        file_path = '/tmp/'
        top_three_locations.to_csv(f'{file_path}{file_name}', sep=',', encoding='utf-8', index=False, header=False)
