
import logging
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

class DBokarevRickMortyHook2(HttpHook):
    """Rick & Morty API"""
    def __init__(self, http_conn_id: str, **kwargs)->None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method='GET'
    def get_pages_count(self):
        """Returns count of pages in location API"""
        return self.run('api/location').json()['info']['pages']

    def get_page_by_num(self, page_num: str)->list:
        """Returns page with page_num from location API"""
        return self.run(f'api/location?page={page_num}').json()['results']

    def get_locations_info(self, loc_num: str)->list:
        """Returns locations info by list of numbers"""
        return  self.run(f'api/location/{loc_num}').json()

class DBokarevRickMortyOperator2(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.loc_dict = {}

    def get_residents_count(self,result_json: list)->None:
        """"Counts residents by every location on page"""
        for location in result_json:
            self.loc_dict[location.get('id')]=len(location.get('residents'))

    def create_table(self):
        hook= PostgresHook('conn_greenplum_write')
        try:
            with hook.get_conn() as conn:
                with conn.cursor() as curs:
                    curs.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'd_bokarev_ram_location';")
                    if curs.fetchone()[0] == 0:
                        curs.execute('create table d_bokarev_ram_location(id text, name text, type text, dimension text, resident_cnt text);')
        finally:
            conn.close()

    def execute(self, context):
        """
         Logging count of dead or alive in Rick&Morty
        """
        hook = DBokarevRickMortyHook2('dina_ram')
        for page in range(hook.get_pages_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_page_by_num(str(page + 1))
            self.get_residents_count(one_page)
        logging.info('Locations dictionary')
        logging.info(self.loc_dict)
        self.create_table()
        top3 = list(dict(sorted(self.loc_dict.items(), key=lambda item: item[1], reverse=True)).keys())[:3]
        loc_tbl=[]
        for loc in hook.get_locations_info(top3):
            loc_tbl.append({'id': loc['id'],
                            'name': loc['name'],
                            'type': loc['type'],
                            'dimension': loc['dimension'],
                            'resident_cnt': len(loc['residents'])})
        df=pd.DataFrame(loc_tbl, columns=['id','name', 'type', 'dimension', 'resident_cnt'])
        logging.info(df)
        df.to_csv('/tmp/d_bokarev_ram.csv', sep=',', header=False, index=False)
        hook = PostgresHook('conn_greenplum_write')
        try:
            with hook.get_conn() as conn:
                with conn.cursor() as curs:
                    curs.execute("truncate d_bokarev_ram_location;")
        finally:
            conn.close()

        logging.info('Start copy data from csv...')
        hook.copy_expert("COPY d_bokarev_ram_location FROM STDIN DELIMITER ','", '/tmp/d_bokarev_ram.csv')
        logging.info('Success!')