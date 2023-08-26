import logging
import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class A_Zakirova_17_Top3Locations(BaseOperator):

    def __init__(self, top_num: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_num = top_num

    def get_page_count(self, api_url: str) -> int:
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_locations(self, result_json: list) -> dict:
        all_locations = []

        for location in result_json:
            all_locations.append([location.get('id'), location.get('name'), location.get('type'),
                                  location.get('dimension'), len(location.get('residents'))])
        logging.info(all_locations)
        return all_locations

    def create_sql_values(self, loc_list: list):
        sql_values = ''
        for el in loc_list:
            sql_values += f"({el[0]}, '{el[1]}', '{el[2]}', '{el[3]}', {el[4]})"
        return sql_values

    def execute(self, context):
        all_locations = []
        ram_loc_api = 'https://rickandmortyapi.com/api/location?page={page}'

        for page in range(self.get_page_count(ram_loc_api.format(page='1'))):
            r = requests.get(ram_loc_api.format(page=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                all_locations.extend(self.get_locations(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        top_locations = sorted(all_locations, key=(lambda l: l[4]), reverse=True)[:self.top_num]

        logging.info(top_locations)
        logging.info(self.create_sql_values(top_locations))

        return self.create_sql_values(top_locations)