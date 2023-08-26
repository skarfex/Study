import logging
import requests
from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class MoshninaRamLocationsOperator(BaseOperator):
    """
    Find top 3 Rick and Morty locations and write them to g-moshnina_ram_locations
    in students DB
    """

    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # хочу тут получать название таблицы

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of pages in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            print(f'page_count = {page_count}')
            return page_count
        else:
            logging.info("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_page_locations(self, result_json: list) -> dict:
        """
        Get dict of all locations on the current page
        :param result_json
        :return: page_locations
        """
        page_locations = {}
        for one_location in result_json:
            page_locations[one_location.get('id')] = [one_location.get('name'), one_location.get('dimension'), 
                                                len(one_location.get('residents'))] 
        return page_locations

    def execute(self, context):
        """
        Writes top 3 locations by number of residents to gp
        """

        all_locations = {}
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'

        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                for k, v in self.get_page_locations(r.json().get('results')).items():
                    all_locations[k] = v

            else:
                logging.info("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick and Morty API')
    
        top3_nresidents = sorted([v[2] for v in all_locations.values()], reverse=True)[:3]
        keys = [k for k, v in all_locations.items() if v[2] in top3_nresidents]
        top_locations = {k: all_locations[k] for k in keys}
        self.top_locations = top_locations
        logging.info(f"top 3 locations {top_locations}")

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('TRUNCATE public.g_moshnina_top_ram_locations;', False)
        
        for k, v in top_locations.items():
            pg_hook.run(f"INSERT INTO public.g_moshnina_top_ram_locations VALUES\
                 ({k}, '{v[0]}', '{v[1]}', {v[2]})", False)

        logging.info('Locations are inserted into gp table')
