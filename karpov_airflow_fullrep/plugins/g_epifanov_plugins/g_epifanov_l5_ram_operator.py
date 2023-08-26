import pandas as pd
import requests
import logging
import json

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class g_epifanov_top_location_operator(BaseOperator):
    '''
    Выгрузка ТОП-3 локации из API Rick&Morty
    '''

    ui_color = '#C2F2F0'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    # Количество страниц на выгрузку
    def get_count_of_page(self, url: str) -> int:
        r = requests.get(url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'pages:  {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    # Определить ТОП-3 локации
    def execute(self, context):

        url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        locations = {'id': [], 'name': [], 'type': [], 'dimension': [], 'resident_cnt': []}

        # Заполнение словаря
        for page in range(self.get_count_of_page(url.format(pg='1'))):
            r = requests.get(url.format(pg=str(page + 1))).json().get('results')
            for i in r:
                locations['id'].append(i['id'])
                locations['name'].append(i['name'])
                locations['type'].append(i['type'])
                locations['dimension'].append(i['dimension'])
                locations['resident_cnt'].append(len(i['residents']))

        df = pd.DataFrame.from_dict(locations)
        df = df.sort_values('resident_cnt', ignore_index=True, ascending=False).head(3)
        tuples = [tuple(x) for x in df.to_numpy()]
        return ','.join(map(str, tuples))










