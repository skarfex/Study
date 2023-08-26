import logging
import requests
import pandas as pd

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class VorobevNRamOperator(BaseOperator):

    def __init__(self, api_url: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.api_url = api_url

    def get_info_from_api_func(self) -> int:
        info = requests.get(self.api_url)
        if info.status_code == 200:
            logging.info('Request OK!')
            cnt = info.json().get('info').get('pages')
            logging.info(f'page_count = {cnt}')
            return cnt
        else:
            logging.warning("HTTP STATUS {}".format(info.status_code))
            raise AirflowException('Error in load page count')

    def info_from_all_pages_func(self, locations: list):
        all_info_page = pd.DataFrame()
        logging.info('Making DF from page')
        for location in locations:
            all_info_page = all_info_page.append(
                {
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents'])
                },
                ignore_index=True
            )
        logging.info(all_info_page)
        return all_info_page

    def execute(self, context):
        all_info = pd.DataFrame()

        for page in range(self.get_info_from_api_func()):
            info = requests.get(self.api_url + '?page=' + str(page + 1))
            if info.status_code == 200:
                logging.info(f'Page {page + 1} in progress...')
                all_info = all_info.append(self.info_from_all_pages_func(info.json()['results']), ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(info.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        logging.info('All pages DONE')
        logging.info('Starting sort')

        all_info = all_info.sort_values(by='resident_cnt', ascending=False)[:3]
        logging.info(f'top-3 locations: {all_info}')
        context['ti'].xcom_push(value=all_info, key='n-vorobev-ram')