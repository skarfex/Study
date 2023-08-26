import requests
import logging
import io
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

class ASTopLocationsOperator(BaseOperator):

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


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

    def get_locations_on_page(self, result_json: list) -> int:
        locations_on_page = []
        for one_char in result_json:
            locations_on_page.append((one_char.get('id'), one_char.get('name'), one_char.get('type'),
                                      one_char.get('dimension'), len(one_char.get('residents'))))

        logging.info(locations_on_page)
        return locations_on_page

    def execute(self, context):
        locations = []
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations += self.get_locations_on_page(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        locations.sort(key=lambda tup: tup[4], reverse=True)

        top =locations[:3]

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        csv_io = io.StringIO()
        df_top = pd.DataFrame(top)
        df_top.to_csv(csv_io, sep='\t', header=False, index=False)
        csv_io.seek(0)

        cursor.copy_from(csv_io, 'ale_suchilin_ram_location')
        logging.info('SQL INSERT: ')
        logging.info(top)
        conn.commit()

        cursor.close()
        conn.close()
