"""
Оператор, который считает количество резидентов в каждой локации
и выбирает top-3 локаций по количеству резидентов
"""

import requests
import logging
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class SAstafeva19RAMResLocOperator(BaseOperator):

    ui_color = "turquoise"

    def __init__(self, top_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_loc: int = top_locations
        assert self.top_loc > 0, "Number of top locations should be > 0"


    def get_location_count(self, api_url: str):
        """
        Get count of locations from API
        :param api_url
        :return: locations count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info(f"SUCCESS connection to {api_url}")
            loc_count: int = r.json().get('info').get('count')
            logging.info(f'locations count = {loc_count}')
            return loc_count
        else:
            logging.warning(f"HTTP STATUS {r.status_code}")
            raise AirflowException('Error in load page count')

    
    def get_residents_count(self, loc_api_url: str):
        """
        Get count of residents from location API
        :param loc_api_url
        :return: residents count
        """
        r = requests.get(loc_api_url)
        if r.status_code == 200:
            logging.info(f"SUCCESS connection to {loc_api_url}")
            residents_count: int = len(r.json()['residents'])
            logging.info(f'residents count = {residents_count}')
            return residents_count
        else:
            logging.warning(f"HTTP STATUS {r.status_code}")
            raise AirflowException('Error in load page count')


    def get_top_locations(self, lst_counts: list) -> list:
        """
        Get ids of locations from list of tuples(id, residents_count)
        :param list of tuples
        :return: self.top ids of locations
        """
        top_tuples: list = sorted(lst_counts, key=lambda x: -x[1])[:self.top_loc]
        logging.info(f'Get top {self.top_loc} locations {top_tuples}')
        return [x[0] for x in top_tuples]


    def get_info_from_location(self, loc_api_url: str):
        """
        Get id, name, type, dimension, resident_cnt from location API
        :param loc_api_url
        :return: full location info from api_url
        """
        r = requests.get(loc_api_url)
        if r.status_code == 200:
            logging.info(f"SUCCESS connection to {loc_api_url}")
            loc_id: int = r.json()['id']
            name: str = r.json()['name']
            loc_type: str = r.json()['type']
            dimension: str = r.json()['dimension']
            residents_count: int = len(r.json()['residents'])
            logging.info(f'Current location info: {loc_id}, {name}, {loc_type}, {dimension}, {residents_count}')
            return (loc_id, name, loc_type, dimension, residents_count)
        else:
            logging.warning(f"HTTP STATUS {r.status_code}")
            raise AirflowException('Error in load page count')


    def execute(self, context):
        """
        Write top locations to db
        """
        ram_url: str = 'https://rickandmortyapi.com/api/location'
        loc_count: int = self.get_location_count(ram_url)
        assert self.top_loc <= loc_count, f"Number of top locations should be less than {loc_count}"
        
        # Посчитаем количество резидентов в каждой локации
        lst_counts: list = list()
        for i in range(loc_count):
            loc_api_url: str = f'{ram_url}/{i + 1}'
            residents_count: int = self.get_residents_count(loc_api_url)
            lst_counts.append((i + 1, residents_count))
        
        # Отберем топ локаций
        top_list: list = self.get_top_locations(lst_counts)

        # Получим список полной информации по топовым локациям
        top_loc_info: list = list()
        for loc in top_list:
            loc_api_url: str = f'{ram_url}/{loc}'
            loc_info: tuple = self.get_info_from_location(loc_api_url)
            top_loc_info.append(str(loc_info))
        
        # Вернем список значений, которые надо записать в таблицу
        sql_values: str = ", ".join(top_loc_info)
        logging.info(f'{sql_values} will be send to xcom')
        context['ti'].xcom_push(key='top_loc', value=sql_values)

            


    

