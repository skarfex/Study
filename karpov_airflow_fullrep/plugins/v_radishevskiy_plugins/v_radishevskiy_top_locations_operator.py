import requests
import logging

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
# from airflow.providers.http.hooks.http import HttpHook


class FetchTopLocations(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __fetch_all_locations(self):
        results = []
        url_location = 'https://rickandmortyapi.com/api/location'
        logging.info(f'Create session')
        with requests.Session() as session:
            while url_location:
                logging.info(f'Fetching {url_location}')

                response = session.get(url_location)
                response.raise_for_status()

                data = response.json()
                url_location = data.get('info', {}).get('next')
                results.extend(data.get('results', []))

            logging.info(f'Total results {len(results)}')
        return results

    def execute(self, context):
        locations = self.__fetch_all_locations()
        locations = [
            dict(
                id=_['id'],
                name=_['name'],
                type=_['type'],
                dimension=_['dimension'],
                resident_cnt=len(_['residents'])
            ) for _ in locations]
        top_locations = sorted(locations, key=lambda _: _['resident_cnt'], reverse=True)[:3]

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO "v-radishevskiy_ram_location"(id, name, type, dimension, resident_cnt)
                    VALUES(%s,%s,%s,%s,%s)""", [
                        (_['id'], _['name'], _['type'], _['dimension'], _['resident_cnt'])
                        for _ in top_locations]
                )
                conn.commit()
