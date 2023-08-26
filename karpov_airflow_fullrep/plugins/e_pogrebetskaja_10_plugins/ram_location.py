import requests
import pandas as pd
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class EPogrebetskayaRamLocationOperator(BaseOperator):
    """
    Top 3 location
    """
    template_fields = ('n',)
    ui_color = "#e0ffff"

    def __init__(self, n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n = n

    def get_page_count(self, api_url: str) -> int:
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
        """
        Save to csv top n locations by number residents in Rick&Morty
        """
        location_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        api_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            tmp_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
            r = requests.get(api_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                data = r.json()
                for i, location in enumerate(data['results']):
                    tmp_df.loc[i, :] = [location['id'],
                                        location['name'],
                                        location['type'],
                                        location['dimension'],
                                        len(location['residents'])]
                location_df = pd.concat([location_df, tmp_df])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        location_df.sort_values(by='resident_cnt', ascending=False)\
            .head(self.n)\
            .to_csv('/tmp/e-pogrebetskaja-10_ram_locations.csv', index=False, header=False)
