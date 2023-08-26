'''
Оператор, посвященный заданию 5-го урока из блока "ETL"
Оператор возвращает из API Rick&Morty ТОП-N локаций по количеству резидентов
"""
'''

import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class TOPLocationOperator(BaseOperator):

    template_fields = ('top_num',) # определяем кортеж объектов, которые могут быть шаблонизированы (заданы в таске)
    ui_color = "#d7a3ff" # задаем цвет оператора

    def __init__(self, top_num: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_num = top_num

    # Подсчет страниц
    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url) # записываем объект response в переменную r
        if r.status_code == 200: # status_code == 200 - ответ от сервера успешно получен
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages') # получаем кол-во страниц
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')  # генерация исключения в airflow2

    # Получение информации о каждой локации в пределах одной страницы
    def get_location_info_on_page(self, result_json: list) -> list:
        """
         Get info about each location on one page
         :param result_json (list results on one page)
         :return species_count (list of tuples that contain info about each location on one page)
        """
        location_info_on_page = []
        for record in result_json:
            location_info_on_page.append((
                record.get('id'),
                record.get('name'),
                record.get('type'),
                record.get('dimension'),
                len(record.get('residents'))
            ))
        return location_info_on_page

    # Получение информации обо всех локациях
    def get_location_info(self):
        """
        Get info about all locations in API
        :param
        :return species_count (list of tuples that contain info about each location in API)
        """
        location_info = []
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                location_info.extend(self.get_location_info_on_page(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return location_info

    # Фильтрация ТОП-N локаций по количеству резидентов
    def execute(self, context):
        """
        Get info about TOP-top_num locations by count of residents
        :param
        :return species_count (list of tuples that contain info about each location in API)
        """
        location_info = self.get_location_info()

        # Проверка, что top_num меньше или равно, чем общее кол-во локаций
        if self.top_num > len(location_info):
            raise ValueError("The specified top_num is greater than the total number of locations.")

        top_location = sorted(location_info, key=lambda x: x[4], reverse=True)
        logging.info(top_location[:self.top_num])
        return top_location[:self.top_num]

    # TODO: добавить проверку на то, что входной параметр top_num меньше, чем общее количество локаций
    # TODO: было бы круто параметр по умолчанию установить top_num = кол-во всех локаций