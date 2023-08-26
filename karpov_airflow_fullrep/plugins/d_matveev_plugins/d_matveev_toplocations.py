import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

import pandas as pd

class DMatveevTopLocationsOperator(BaseOperator):
    template_fields = ('top_n', 'conn_id', 'table_name',)

    def __init__(self, top_n, conn_id, table_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n
        self.conn_id = conn_id
        self.table_name = table_name

    def get_page_count(self, api_url: str) -> int:
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')
            
 
             
    
    def get_human_count_on_page(self, result_json: list):
        location_info_df = {'id': [], 'name': [], 'type': [], 'dimension': [], 'resident_cnt': []}
        location_info_df = pd.DataFrame(data=location_info_df)
        for one_char in result_json:
            location_info = [one_char.get('id'), one_char.get('name'), one_char.get('type'), 
                         one_char.get('dimension'), len(one_char.get('residents'))]
            df = pd.DataFrame([location_info], columns=["id","name","type","dimension", "resident_cnt"])
            location_info_df = pd.concat([location_info_df, df])
        return location_info_df

    
    def load_ram_func(self):
        """
        Logging count of Human in Rick&Morty
        """
        all_pages_location_info_df = {'id': [], 'name': [], 'type': [], 'dimension': [], 'resident_cnt': []}
        all_pages_location_info_df = pd.DataFrame(data=all_pages_location_info_df)    
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                df = self.get_human_count_on_page(r.json().get('results'))
                all_pages_location_info_df = pd.concat([all_pages_location_info_df, df])      
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        logging.info(f'Humans in Rick&Morty: {all_pages_location_info_df}')
        all_pages_location_info_df = all_pages_location_info_df.sort_values(by=['resident_cnt'], ascending = False).head(3)
        all_pages_location_info_df.to_csv('/tmp/d_matveev_ram.csv', sep=',', header=False, index=False)
        return all_pages_location_info_df
    
        


    def execute(self, context):
        self.load_ram_func()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        pg_hook.copy_expert("COPY d_matveev_ram_location FROM STDIN DELIMITER ','", '/tmp/d_matveev_ram.csv')
            
         