"""
три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

"""

import logging
from airflow.models import BaseOperator
import requests
import pandas as pd
from airflow.exceptions import AirflowException


class s_dobrynin_ram_top3_op(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, url_page: str) -> int: #подсчет количества страниц.
        r = requests.get(url_page)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def execute(self, **kwargs):
        url_location= 'https://rickandmortyapi.com/api/location?page={pg}'
        df = pd.DataFrame(columns=['id','name', 'type', 'dimension', 'resident_cnt'], dtype='object')

        for page in range(self.get_page_count(url_location.format(pg='1'))):
            r = requests.get(url_location.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                res = r.json().get('results')
                #Подсчет всех страниц и запись в датафрейм из jsona res
                for i in range(len(res)):
                    df.loc[i] = [res[i]['id'],
                                res[i]['name'],
                                res[i]['type'],
                                res[i]['dimension'],
                                len(res[i]['residents'])]
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        df = df.sort_values(by='resident_cnt', ascending=False).head(3) #top 3
        return df

