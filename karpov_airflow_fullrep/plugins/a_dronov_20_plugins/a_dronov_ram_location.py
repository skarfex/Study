import requests
import logging

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException


class ADRONOV20RamTopLocationCountOperator(BaseOperator):
    """
    Count top-3 resident location
    """

    ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
    loc_insert = """INSERT INTO a_dronov_20_ram_location (id, name, type, dimension, resident_cnt)
                      VALUES (%s, %s, %s, %s, %s);"""
    ui_color = '#e0ffe6'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    # Определяем общее кол-во страниц с данными
    def get_page_cnt(self):
        r = requests.get(self.ram_char_url.format(pg='1'))

        if r.status_code == 200:
            logging.info("GET_PAGE. SUCCESS.")
            page_cnt = r.json().get('info').get('pages')
            return page_cnt
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('GET_PAGE. ERROR.')

    # Определяем список с данными по локациям.
    # Возвращаем только три локации с наибольшим кол-вом резидентов.
    def load_ram_func(self):
        res_table = []

        for cur_page in range(self.get_page_cnt()):
            r = requests.get(self.ram_char_url.format(pg=str(cur_page + 1)))
            if r.status_code == 200:
                logging.info(f"LOAD_RAM. PAGE = {cur_page}. SUCCESS.")
                for loc in r.json().get('results'):
                    res_table.append(
                        {
                            'id': loc.get('id'),
                            'name': loc.get('name'),
                            'type': loc.get('type'),
                            'dimension': loc.get('dimension'),
                            'resident_cnt': len(loc.get('residents'))
                        }
                    )
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('LOAD_RAM. ERROR.')

        return sorted(res_table, key=lambda count: count['resident_cnt'], reverse=True)[:3]

    # Подключаемся к ГП и вставляем те локации, в которых больше всего резидентов (топ-3)
    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        for value in self.load_ram_func():
            pg_hook.run(self.loc_insert, parameters=(int(value['id']),
                                                     value['name'],
                                                     value['type'],
                                                     value['dimension'],
                                                     int(value['resident_cnt']))
                        )
