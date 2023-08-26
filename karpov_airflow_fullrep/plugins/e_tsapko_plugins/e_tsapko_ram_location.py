import logging
import requests
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class ETsapkoRamLocationOperator(BaseOperator):
    """
    Count number of residents in each location
    On ETsapkoRamLocationHook
    And inserts the top given number of locations
    To the Greenplum
    """

    template_fields = ('top_n', 'ascending',)
    ui_color = "#c7ffe9"

    df_locations = pd.DataFrame()
    columns = ['id', 'name', 'type', 'dimension', 'resident_cnt']

    def __init__(self, top_n: int = 3, ascending: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n
        self.ascending = ascending

    def get_pages_count(self, api_url: str) -> int:
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

    def extract(self):
        """
        Extracts data from Rick&Morty API to `df_locations` DataFrame.
        """
        ram_locations_url = "https://rickandmortyapi.com/api/location?page={page}"
        for page in range(self.get_pages_count(ram_locations_url.format(page='1'))):
            r = requests.get(ram_locations_url.format(page=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                self.df_locations = pd.concat([self.df_locations, pd.DataFrame(r.json().get('results'))])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API to DataFrame')
        logging.info("Successfully loaded")

    def transform(self):
        """
        Adds `resident_cnt` column with amount of residents in each location to the `df_locations`,
        and saves filtered (`top_n` rows ordered by `resident_cnt` [asc/desc]) dataframe to csv.
        """
        self.df_locations['resident_cnt'] = self.df_locations.apply(lambda row: len(row['residents']), axis=1)
        self.df_locations[self.columns].sort_values(
            by=['resident_cnt'],
            ascending=self.ascending
        ).head(self.top_n).to_csv("/tmp/e_tsapko_ram_locations.csv", index=False, header=False)

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        self.extract()
        self.transform()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        pg_hook.copy_expert("COPY public.e_tsapko_ram_location FROM STDIN DELIMITER ','", '/tmp/e_tsapko_ram_locations.csv')
