from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
import logging
import requests


class DaaRNMOperator(BaseOperator):

    template_fields = ('top_location_count',)

    def __init__(self, top_location_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        if top_location_count < 0:
            logging.warning("Значение top_location_count меньше ноля, переменной присвоено значение 3")
            self.top_location_count = 3
        self.top_location_count = top_location_count

    def get_page_count(self, api_url) -> int:

        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("Успешно")
            page_count = r.json()['info']['pages']
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_top_location(self, api_url) -> list:
        locations = []
        for i in range(self.get_page_count(api_url.format(page='1'))):
            locations += requests.get(api_url.format(page=str(i + 1))).json()['results']
        return locations

    def execute(self, context):
        rnm_location_url = 'https://rickandmortyapi.com/api/location?page={page}'
        sorted_location = sorted(self.get_top_location(rnm_location_url),
                              reverse=True,
                              key=lambda element: len(element['residents']))

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('TRUNCATE TABLE a_dudin_16_ram_location', False)
        sql_statement = 'INSERT INTO a_dudin_16_ram_location VALUES'
        if self.top_location_count > len(sorted_location):
            logging.warning("Значение top_location_count больше количества локаций в RNM")

        for i in range(min(self.top_location_count, len(sorted_location))):
            if i > 0:
                sql_statement += ','
            sql_statement += f'''( {sorted_location[i]['id']},\
            '{sorted_location[i]['name']}',\
            '{sorted_location[i]['type']}',\
            '{sorted_location[i]['dimension']}',\
            {len(sorted_location[i]['residents'])} \
            )
            '''
        pg_hook.run(sql_statement, False)