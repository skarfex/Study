
import requests
import logging
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class MEvgeniia21TopLocationsOperator(BaseOperator):
    """
    Find top-3 locations by number of residents and upload them to database
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_location_json(self, api_url: str) -> list:
        """
        Get all locations from pages and merge them to one json
        :param api_url:
        :return: location_json
        """
        location_json = []
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                result_json = r.json().get('results')
                for item in result_json:
                    item['resident_cnt'] = len(item['residents'])
                location_json += result_json
                logging.info(f'Location from {page + 1} were added to json')
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                print('Error')
        return location_json

    def upload_locations_to_gp(self, location_json: list):
        """
        Create table in DataBase and upload there top-3 locations
        """

        df = pd.DataFrame(location_json).sort_values(by=['resident_cnt'], ascending=False)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS public.m_evgeniia_21_ram_location (
                id integer unique,
                name varchar, 
                type varchar, 
                dimension varchar, 
                resident_cnt integer);
        """

        try:
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
            logging.info(f"Creating a table 'm_evgeniia_21_ram_location'")
            pg_hook.run(create_table_sql, False)
            location_df = df[['id', 'name', 'type', 'dimension', 'resident_cnt']].iloc[:3]
            logging.info(f"{location_df} are adding to database...")
            location_df.to_sql('m_evgeniia_21_ram_location',
                               con=pg_hook.get_sqlalchemy_engine(),
                               if_exists='replace',
                               index=False)
            logging.info(f'Locations were added successfully')
        except:
            raise AirflowException('Error')

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        api_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        location_json = self.get_location_json(api_url)
        self.upload_locations_to_gp(location_json)

