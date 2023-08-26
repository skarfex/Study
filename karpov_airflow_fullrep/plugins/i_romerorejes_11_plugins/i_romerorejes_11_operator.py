
import requests
import logging
import pandas as pd


from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

class TopLocations(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # Получаем количество страниц
    def get_page_count(self, api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        # Забираем данные и складываем в датафрейм
        api_url = 'https://rickandmortyapi.com/api/location'
        all_results = []
        for page in range(1, self.get_page_count(api_url)):
            page_url = f'https://rickandmortyapi.com/api/location?page={page}'
            r = requests.get(page_url)
            data = r.json()['results']
            result = []
            for location in data:
                locations_res = {
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents'])}
                all_results.append(locations_res)
        result_df = pd.DataFrame(all_results)
        logging.info(result_df)
        result_top3_df = result_df.sort_values('resident_cnt', ascending=False).head(3)
        result_top3_df.to_csv('/tmp/i-romerorejes-11_top_locations.csv', sep=',', header=False, index=False)


        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY i_romerorejes_11_rickandmorty_top_locations FROM STDIN DELIMITER ','", '/tmp/i-romerorejes-11_top_locations.csv')
        logging.info('Данные загружены')