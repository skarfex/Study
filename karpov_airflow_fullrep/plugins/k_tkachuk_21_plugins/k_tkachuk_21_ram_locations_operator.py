"""
m4-l5_airflow
homework
"""

import logging
import requests
import json

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class KTkachuk21RamLocationOperator(BaseOperator):

    api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    template_fields = ("num_of_locations",)
    ui_color = 'f5fffa'

# По умолчанию задаем поиск по топ-3
    def __init__(self, num_of_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.num_of_locations = num_of_locations

# Забираем ин-фу по общему кол-ву страниц
    def num_of_pages(self):
        req = requests.get(self.api_url.format(pg='1'))
        if req.status_code == 200:
            logging.info("SUCCESS")
            page_count = req.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(req.status_code))
            raise AirflowException('Error in load page count')

# Собираем ин-фу для таблицы в GP по каждой странице, считаем кол-во резидентов
# Сортируем по кол-ву резидентов и отбираем топ-3
# resident_cnt — длина списка в поле residents
    def top_locations(self, num_of_locations):
        result_table = []
        for page in range(self.num_of_pages()):
            req = requests.get(self.api_url.format(pg=str(page+1)))
            if req.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                converted_json = json.loads(req.text)['results']
                for column in converted_json:
                    converted_location = {
                        'id': column['id']
                        , 'name': column['name']
                        , 'type': column['type']
                        , 'dimension': column['dimension']
                        , 'resident_cnt': len(column['residents'])
                    }
                    result_table.append(converted_location)
            else:
                logging.warning("HTTP STATUS {}".format(req.status_code))
                raise AirflowException('Error in load info about resident count')
        return sorted(result_table, key=lambda count: count['resident_cnt'], reverse=True)[: num_of_locations]

# Используем соединение 'conn_greenplum_write'
    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        result_data = []
        for value in self.top_locations(self.num_of_locations):
            prepared = f"({value['id']}, \'{str(value['name'])}\', \'{str(value['type'])}\', \'{str(value['dimension'])}\', {value['resident_cnt']})"
            result_data.append(prepared)

# Записываем значения соответствующих полей этих трёх локаций в таблицу
        sql_query = f'''INSERT INTO public.k_tkachuk_21_ram_location VALUES {','.join(result_data)}'''
        cursor.execute(sql_query)
        conn.commit()
