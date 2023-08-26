import requests
import logging
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RickMortyFindTopLocOperator(BaseOperator):
    loc_url = 'https://rickandmortyapi.com/api/location/'
    template_field = ('target',)

    def __init__(self, target: int = 3, **kwargs):
        super().__init__(**kwargs)
        self.target = target

    def cnt_resident(self, list_rep) :
        for i in list_rep:
            i['residents'] = len(i['residents'])
        return list_rep

    def get_top_list(self, url, target):
        r = requests.get(url)
        list_loc = []
        if r.status_code == 200:
            logging.info("SUCCESS")
            pg_cnt = r.json()['info']['pages']
            logging.info(f'page_count = {pg_cnt}')
            for pg in range(0, pg_cnt):
                list_loc += requests.get(f'{url}?page={pg + 1}').json()['results']
            list_loc = self.cnt_resident(list_loc)
            list_loc.sort(reverse=True, key=lambda element: element['residents'])
            return list_loc[:target]
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        for i in self.get_top_list(self.loc_url, self.target):
            sql_stmt = f"INSERT INTO d_evteev_ram_location VALUES ( \
                        {i['id']}, '{i['name']}', '{i['type']}', '{i['dimension']}', {i['residents']})"
            logging.info("Inserted into d-evteev_ram_location: " + sql_stmt)
            pg_hook.run(sql_stmt, False)

