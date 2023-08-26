import logging

import pandas as pd
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class GetLocationsInfoOperator(BaseOperator):
    """
    Get locations info (id, name, type, dimensions, residents count) operator.
    """

    def __init__(self, table, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = 'https://rickandmortyapi.com/api/location?page={pg}'
        self.top_n = 3
        self.pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        self.table_name = table

    def _get_page_count(self) -> int:
        """
        Get count of page in API
        :return: page count
        """
        r = requests.get(self.url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')

    def execute(self, context) -> None:
        locations = []
        for page in range(self._get_page_count()):
            r = requests.get(self.url.format(pg=page + 1))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                for result in r.json().get('results'):
                    locations.append({'id': result['id'], 'name': result['name'], 'type': result['type'],
                                      'dimension': result['dimension'], 'resident_cnt': len(result['residents'])})
            else:
                logging.warning(f"HTTP STATUS {r.status_code}")
                raise AirflowException('Error in load from Rick&Morty API')
        df = pd.DataFrame(locations)
        engine = self.pg_hook.get_sqlalchemy_engine()
        df.sort_values('resident_cnt', ascending=False).head(self.top_n).to_sql(self.table_name,
                                                                                con=engine,
                                                                                if_exists='replace',
                                                                                index=False)
