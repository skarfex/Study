from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
import requests
import logging


class RamTopNLocationOperator(BaseOperator):

    template_fields = ('top_location_cnt',)

    def __init__(self, top_location_cnt: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        if top_location_cnt < 0:
            logging.warning("VALUE IN TOP_LOCATION_CNT LESS THAN 0, VARIABLE GET DEFAULT VALUE = 3")
            self.top_location_cnt = 3
        self.top_location_cnt = top_location_cnt

    def get_page_count(self, api_url) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
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
        ram_location_url = 'https://rickandmortyapi.com/api/location?page={page}'
        sorted_location = sorted(self.get_top_location(ram_location_url),
                              reverse=True,
                              key=lambda element: len(element['residents']))

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('TRUNCATE TABLE a_evteev_ram_location', False)
        sql_statement = 'INSERT INTO a_evteev_ram_location VALUES'
        if self.top_location_cnt > len(sorted_location):
            logging.warning("VALUE IN VAR TOP_LOCATION_CNT BIGGER THAN LOCATION'S COUNT IN RAM")

        for i in range(min(self.top_location_cnt, len(sorted_location))):
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
