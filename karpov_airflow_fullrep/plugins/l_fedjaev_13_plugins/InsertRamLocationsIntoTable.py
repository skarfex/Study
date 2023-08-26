# -*- coding: utf-8 -*-
#
# Plugin for AirFlow: RamLocationOperator.py
# Written by Leo Fedyaev, 2022-10-10
#


import logging
import requests

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class InsertRamLocationsIntoTable(BaseOperator):
    """Оператор для извлечения специфической информации"""

    template_fields = ('conn_id', 'table_name', 'loc_limit')

    def __init__(self, conn_id: str, table_name: str, loc_limit: int, **kwargs):
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.loc_limit = loc_limit


    # Rick&Morty API
    def get_ram_locs(self, f, reverse: bool =False, limit: int =-1) -> list:
        """Вернуть отсортированные локации в упрощённом виде"""

        req = requests.get('https://rickandmortyapi.com/api/location')

        if req.status_code != 200:
            return None

        locs = []
        for loc in sorted(req.json().get('results'), key=f, reverse=reverse)[:limit]:
            locs.append(tuple([loc[key]  for key in ['name', 'type', 'dimension']]\
                + [len(loc['residents'])]))

        return locs


    # Перегрузка родительского метода
    def execute(self, context):
        logging.info(f'{self.conn_id}')
        logging.info(f'{self.table_name}')

        gp = PostgresHook(postgres_conn_id=self.conn_id)
        conn = gp.get_conn()
        cursor = conn.cursor()

        # Получить и вставить значения
        locations = self.get_ram_locs(lambda d: len(d['residents']),
                                      reverse=True, limit=self.loc_limit)

        logging.info(f'Locations ({len(locations)}):')

        for index, loc in enumerate(locations, 1):
            logging.info(f'{index}. {loc};')

            query = f'insert into {self.table_name} '\
                     '(name, type, dimension, resident_cnt) values (%s, %s, %s, %s);'
            cursor.execute(query, loc)

        conn.commit()
