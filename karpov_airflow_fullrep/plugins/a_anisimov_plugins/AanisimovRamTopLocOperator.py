from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
import requests
import logging
import csv
class AanisimovRamTopLocOperator(BaseOperator):
    """
    Find 3 top location from RaM
    """
    #template_fields = ('species_type',)
    ui_color = "#e0ffff"
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
    
    def get_page_count(self, URL):
        r = requests.get(URL)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')
    
    def execute(self, context):
        URL = 'https://rickandmortyapi.com/api/location/?page={}'
        r = requests.get(URL)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

        list_ = []
        for i in range(1, self.get_page_count(URL.format(1)) + 1):
            r = requests.get(URL.format(i))
            for res in r.json()['results']:
                list_.append((res['id'], res['name'], res['type'], res['dimension'], len(res['residents'])))
        list_ = sorted(list_, key=lambda x: x[-1], reverse=True)[:3]
        with open('/tmp/aanisimov_ram.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in list_:
                writer.writerow(row)

        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run("TRUNCATE TABLE {table};".format(table='aanisimov_ram_location'))
        pg_hook.copy_expert("COPY aanisimov_ram_location FROM STDIN DELIMITER ','", '/tmp/aanisimov_ram.csv')
