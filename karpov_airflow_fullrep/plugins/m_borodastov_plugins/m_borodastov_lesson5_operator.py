import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class MBRamTopLocationsOperator(BaseOperator):
    """
    Top locations with residents in the RaM series
    """
    def __init__(self, location: int = 3, **kwargs) -> None:
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
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_residents_count_per_loc(self, r):
        """
        Get main info
        """
        locs_length_page = len(r)
        result = []
        for loc_n in range(locs_length_page):
            result.append([r[loc_n]['id'],
                           r[loc_n]['name'],
                           r[loc_n]['type'],
                           r[loc_n]['dimension'],
                           len(r[loc_n]['residents'])])
        return result

    def execute(self, context):

        residents_count = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg=1))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                residents_count += self.get_residents_count_per_loc(r.json().get('results'))
            else:
                print("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        df = pd.DataFrame(residents_count, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

        try:
            postgres_hook = PostgresHook('conn_greenplum_write')
            engine = postgres_hook.get_sqlalchemy_engine()
            df.sort_values('resident_cnt', ascending=False).head(3).to_sql('m_borodastov_ram_location',
                                                                           con=engine,
                                                                           if_exists='replace',
                                                                           index=False)
            logging.info(f'Locations added successfuly!')
        except:
            raise AirflowException('Error')


