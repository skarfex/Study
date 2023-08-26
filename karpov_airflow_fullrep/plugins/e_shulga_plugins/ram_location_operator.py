import requests
import pandas as pd
import logging
from sqlalchemy import create_engine

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class EShulgaRAMLocationOperator(BaseOperator):
    def __init__(self, n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url, verify=False)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        locations = []
        ram_url = "https://rickandmortyapi.com/api/location?page={pg}"
        for page in range(self.get_page_count(ram_url.format(pg='1'))):
            r = requests.get(ram_url.format(pg=str(page + 1)), verify=False).json()
            for l in r.get('results'):
                locations.append([l.get('id'),
                                  l.get('name'),
                                  l.get('type'),
                                  l.get('dimension'),
                                  len(l.get('residents'))])
                logging.info(f'PAGE {page + 1}')
        locations_df = pd.DataFrame(locations, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        try:
            postgres_hook = PostgresHook('conn_greenplum_write')
            engine = postgres_hook.get_sqlalchemy_engine()
            locations_df.sort_values('resident_cnt', ascending=False).head(3).to_sql('e-shulga_ram_location',
                                                                                     con=engine,
                                                                                     if_exists='replace',
                                                                                     index=False)
            logging.info(f'Locations added')

        except:
            raise AirflowException('Error in upload data to table')