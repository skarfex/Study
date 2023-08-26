import requests
import logging
import json

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.models.xcom import XCom


class SMarkevichRamResidentFromLocation(BaseOperator):
    """
        Отбирает в api RaM 3 локации с максимальным населением
        Готовит данные: id, name, type, dimension, resident_cnt
        Отправляет в пространство Airflow

    """

    template_fields = ('species_type',)
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.start_url = 'https://rickandmortyapi.com/api/location'
        self.three_locals = []
        self.template_values = "({{id}}, {{name}}, {{type}}, {{dimension}}, {{resident_cnt}})"

    def add_if_max_residents_from_three_locations(self, local: dict) -> None:
        if len(self.three_locals) >= 3:
            if self.three_locals[-1]['resident_cnt'] < len(local['residents']):
                self.insert_local(local)
        else:
            self.insert_local(local)

    def insert_local(self, local: dict) -> None:
        self.three_locals.insert(0,{
            "id": local['id'],
            "name": local['name'],
            "type": local['type'],
            "dimension": local['dimension'],
            "resident_cnt": len(local['residents']),
        })
        logging.info(f"added location id:{local['id']},"
                     f" name:{local['name']},"
                     f" count residents:{len(local['residents'])}")

        self.three_locals.sort(key=lambda x: x['resident_cnt'], reverse=True)
        if len(self.three_locals) > 3:
            drop_local = self.three_locals.pop()
            logging.info(f"drop location id:{drop_local['id']},"
                         f" name:{drop_local['name']},"
                         f" count residents:{len(drop_local['residents'])}")

    @staticmethod
    def get_data(url: str) -> dict:
        r = requests.get(url)
        if r.status_code >= 300:
            raise f'response error status code: {r.status_code}, {r.content}'
        logging.info(f'response status code: {r.status_code}')

        return r.json()

    def execute(self, context) -> None:
        data = self.get_data(self.start_url)
        while data['info']['next']:
            for local in data['results']:
                self.add_if_max_residents_from_three_locations(local)
            data = self.get_data(data['info']['next'])

        logging.info(f'result tasks {self.three_locals}')

        data_for_sql = {
            'columns': self.template_values.replace('{{', '').replace('}}', ''),
            'values': ',\n'.join([self.template_values.format(**row) for row in self.three_locals])
        }

        context['ti'].xcom_push(value=data_for_sql['columns'], key='columns')
        context['ti'].xcom_push(value=data_for_sql['values'], key='values')


