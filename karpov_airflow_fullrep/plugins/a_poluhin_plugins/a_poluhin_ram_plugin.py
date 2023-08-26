import requests
import logging
import pandas as pd
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class TopLocationsOperator(BaseOperator):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        locations_dict = {'id':[], 'name':[], 'type':[], 'dimension':[], 'resident_cnt': []}
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1))).json().get('results')
            for sub_data in r:
                locations_dict['id'].append(sub_data['id'])
                locations_dict['name'].append(sub_data['name'])
                locations_dict['type'].append(sub_data['type'])
                locations_dict['dimension'].append(sub_data['dimension'])
                locations_dict['resident_cnt'].append(len(sub_data['residents']))
        locations_df = pd.DataFrame.from_dict(locations_dict)
        locations_df = locations_df.sort_values('resident_cnt', ignore_index=True, ascending=False).head(3)

        ## Придумать, что возвращать для записи в таблицу 
        tuples = [tuple(x) for x in locations_df.to_numpy()]
        full_text = ','.join(map(str, tuples))
        return full_text

    
  