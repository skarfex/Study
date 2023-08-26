import json
import pandas as pd
import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class RMLTopThreeLocationsOperator(BaseOperator):
    '''
    Get data from API about three locations with the most residents
    '''
    ui_color = "#e0ffff"
    
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    
    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise Exception('Error in load page count')
    
    
    def execute(self, context):
        '''
        Find three locations with the most residents
        
        :param None
        :return pd.DataFrame
        '''
        dct = {}
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                universe = r.json().get('results')
                for location_data in universe:
                    dct[location_data['id']] = {
                        'name': location_data['name'],  
                        'type': location_data['type'],
                        'dimension': location_data['dimension'],
                        'resident_cnt': len(location_data['residents'])
                        }
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load page count')
        self.df = pd.DataFrame.from_dict(dct).T.sort_values('resident_cnt', ascending=False)
        
        logging.info(f'Find dataframe, which look like this:')
        logging.info(f'{self.df.head(3)}')
        self.df = self.df.head(3).reset_index()
        self.df.columns = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        self.df.head(3).to_csv('/tmp/m_repin_ram_location.csv', sep=',', index=False, header=None)
        logging.info(f'{self.df}')
        logging.info('Table was succsseful download to tmp location')