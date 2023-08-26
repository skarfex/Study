import logging
import pandas as pd
import requests

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

class AlpankRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, method="GET", http_conn_id='https://rickandmortyapi.com', **kwargs):
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_location_page_count(self):
        """Returns count of page in API"""
        conn = self.get_connection(self.http_conn_id)
        verify = conn.extra_dejson.get('verify', True)

        # Create a session and set the verification flag
        session = requests.Session()
        session.verify = verify

        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        """Returns a result of one location page in the Rick and Morty API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class AlpankRamResidentsCountOperator(BaseOperator):
    """
    Get top 3 locations in Rick and Morty with the largest number of residents
    """

    ui_color = "#228CBD"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = AlpankRickMortyHook('al-a-21-lesson-5')
        results_df = pd.DataFrame()
        for page in range(hook.get_location_page_count()):
            logging.info(f'PAGE {page + 1}')
            results_page = pd.DataFrame(hook.get_location_page(str(page + 1)))
            results_df = pd.DataFrame(results_page) if results_df.empty else pd.concat([results_df, results_page])
        results_df["resident_cnt"] = results_df["residents"].apply(len)
        results_df = results_df.nlargest(3, "resident_cnt")[["id", "name", "type", "resident_cnt"]]
        return results_df
        # results_df.to_csv("/tmp/al_a_21_ram_location.csv")
        # logging.info('Data Frame with top 3 locations in Rick and Morty universe is saved in temp location')
