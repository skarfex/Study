"""
Плагин с оператором по поиску 3х локаций сериала "Рик и Морти" с наибольшим количеством резидентов.
"""
import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import json


class OMalynkovskyTopLocationsOperator(BaseOperator):
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def download_page(self):
        # Локации забираются единственным запросом, без деления на страницы
        r = requests.get('https://rickandmortyapi.com/api/location')
        locations = 0
        if r.status_code == 200:
            logging.info("SUCCESS")
            # Конвертируем в json, запоминаем только значимую часть ответа
            locations = json.loads(r.text)['results']

        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in loading')

        return locations


    def load_top3_location(self):

        locations = self.download_page()
        if (locations == 0): exit()

        # Соберём локации в массив словарей
        location_list = []
        for location in locations:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)

        # Отсортируем этот массив и выберем три первых элемента
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]

        # Соберём список значений для вставки в таблицу
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in top3_location]


        return  ','.join(insert_values)


    def execute(self, context):
        return self.load_top3_location()





