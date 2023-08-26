"""
Повторяем создание следующих инструментов с лекции
1. Операторов
2. Хуков
3. Сенсоров

—— Цель ——
1. Закрепить пройденный материал
2. Набить руку в создании Операторов

—— Инструменты ——
1. Работаем с The Rick and Morty API - https://rickandmortyapi.com/documentation/#introduction
2. Работаем именно с персонажами - https://rickandmortyapi.com/api/character/?page=19

—— Техническое задание ——
1. Создать функцию, считающую количество людей в сериале
2. Используя написанный код внутри функции создать отдельный оператор для этой цели

—— Алгоритм решения ——
1. Нам нужно знать сколько всего страниц нам нужно будет пройти (сколько их всего?)
2. Пройтись по всем страницам и посчитать людей
3. Выдать результат
"""

# ==============================================
#                   Functions
# ==============================================

# Python
import requests
import logging


# def get_page_count(api_url: str):
#     r = requests.get(api_url)

#     if r.status_code == 200:
#         logging.info("API Connect Success")

#         page_count = r.json().get('info').get('pages')
        
#         logging.info(f"page_count = {page_count}")
#         return page_count
#     else:
#         logging.warning("HTTP STATUS {}".format(r.status_code))
#         raise Exception


# def get_human_count_on_page(result_json):
#     human_count_on_page = 0
#     for character in result_json:
#         if character.get('species') == 'Human':
#             human_count_on_page += 1
#     logging.info(f"human_count_on_page = {human_count_on_page}")
#     return human_count_on_page


# def load_ram_func():
#     human_count = 0
#     ram_characters_url = 'https://rickandmortyapi.com/api/character/?page={pg}'
#     pages = get_page_count(ram_characters_url.format(pg='1'))

#     for page in range(1, pages + 1):
#         url = ram_characters_url.format(pg=str(page))
#         r = requests.get(url)

#         if r.status_code == 200:
#             logging.info(f"PAGE {page}")
#             characters_json = r.json().get('results')
#             human_count += get_human_count_on_page(characters_json)
#         else:
#             logging.warning("HTTP STATUS {}".format(r.status_code))
#             raise Exception
    
#     logging.info(f"Humans in Rick&Morty: {human_count}")
#     return human_count
    

# ==============================================
#                   Operators
# ==============================================
# Airflow
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator


class SdipnerRamSpeciesCountOperator(BaseOperator):
    template_fields = ('species_type', )
    ui_color = "#e0ffff"

    def __init__(self, species_type: str = 'Human', **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str):
        r = requests.get(api_url)

        if r.status_code == 200:
            logging.info("API Connect Success")

            page_count = r.json().get('info').get('pages')
            
            logging.info(f"page_count = {page_count}")
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def get_species_count_on_page(self, result_json):
        species_count_on_page = 0
        for character in result_json:
            if character.get('species') == self.species_type:
                species_count_on_page += 1
        logging.info(f"species_count_on_page = {species_count_on_page}")
        return species_count_on_page


    def execute(self, context):
        species_counter = 0
        ram_characters_url = 'https://rickandmortyapi.com/api/character/?page={pg}'
        pages = self.get_page_count(ram_characters_url.format(pg='1'))

        for page in range(1, pages + 1):
            url = ram_characters_url.format(pg=str(page))
            r = requests.get(url)

            if r.status_code == 200:
                logging.info(f"PAGE {page}")
                characters_json = r.json().get('results')
                species_count += self.get_species_count_on_page(characters_json)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        logging.info(f'{self.species_type} in Rick&Morty: {species_count}')
