
import requests
import logging
import pandas as pd
import os

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from typing import List



class SamRamLocationOperator(BaseOperator):
    """
    Top location in Rick&Morty
    """

    template_fields = ('location_top_numbers',)
    ui_color = "#c7ffe9"

    def __init__(self
                 , location_top_numbers: int = 3
                 , api_main_url: str = "https://rickandmortyapi.com/api/location"
                 , api_page_url: str = "https://rickandmortyapi.com/api/location/{pg}"
                 , to_csv: str = "/tmp/sam_locations_lst.csv"
                 , sep: str = "^"
                 , **kwargs) -> None:
        
        super().__init__(**kwargs)
        self.location_top_numbers = location_top_numbers
        self.main_url = api_main_url
        self.page_url = api_page_url
        self.to_csv = to_csv
        self.sep = sep
        
    # Получить количество всех локаций
    def get_location_count(self) -> int:
        """
        Get count of location in API
        :param self.main_url
        :return: location count
        """
        location_cnt = 0
        r = requests.get(self.main_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            location_cnt = r.json().get('info').get('count')
            logging.info(f'location_count = {location_cnt}')
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load location count')

        return int(location_cnt)
    
    # Получить словарь характеристик локации
    def get_location_item(self, api_page_url: str) -> dict:
        """
        Get item of locations with count of resident in API
        :param self.page_url
        :return: dict of location
        """
        item = {}
        r = requests.get(api_page_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            item = {'id': r.json().get('id')
                    , 'name': r.json().get('name')
                    , 'type': r.json().get('type')
                    , 'dimension': r.json().get('dimension')
                    , 'resident_cnt': len(r.json().get('residents'))}
            logging.info(f"Location ID: {item['id']}; Resident count: {item['resident_cnt']}")
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load resident count')
        
        return item
    
    # Получить список словарей с характеристиками основных локаций
    def get_location_top(self) -> List[dict]:
        """
        Get top location by number of resident in API
        :param self.page_url
        :param self.location_top_nbr
        :return: list of top location
        """
        locations_lst = []
        locations_cnt = self.get_location_count()
        location_url = self.page_url
        top_n = 0
        
        if locations_cnt > 0:
            for page in range(locations_cnt):
                new_location_dct = self.get_location_item(location_url.format(pg=str(page + 1)))
                new_resident_cnt = new_location_dct['resident_cnt']
                if top_n < self.location_top_numbers:
                    locations_lst.append(new_location_dct)
                    top_n+=1
                else:
                    locations_lst = sorted(locations_lst, key=lambda d: d['resident_cnt'])
                    for item_dct in locations_lst:
                        if new_resident_cnt > item_dct['resident_cnt']:
                            del locations_lst[0]
                            locations_lst.append(new_location_dct)
                            break
            locations_lst = sorted(locations_lst, key=lambda d: d['resident_cnt'], reverse=True)
            logging.info("Create the list of top locations") 
        else:
            logging.info('locations_cnt = 0')
            
        return locations_lst
    
    # Запуск таска
    def execute(self, context):
        pd.DataFrame(self.get_location_top()).to_csv(self.to_csv, index=False, header=False, sep = self.sep)
        full_path = os.path.abspath(self.to_csv)
        message = f"Copy top location to path {full_path}"
        logging.info(message)