import logging
import csv
import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


class APetrenkoRickMortyHook(HttpHook):
    """
    Rick and Morty API Hook
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations(self):
        return self.run('api/location').json()['info']['pages']

    def get_location_pages(self, pages: int) -> list:
        return self.run(f'api/location?page={pages}').json()['results']

    def get_single_location(self, location_id: int) -> list:
        return self.run(f'api/location/{location_id}').json()


class APetrenkoGetTopLocationOperator(BaseOperator):
    """
    Rick and Morty API Operator
    """
    ui_color = "#e0ffff"


    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_for_pages = {}

    def create_table(self):
        pg_hook = PostgresHook('conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"select * from information_schema.tables t where t.table_name = "
                            f"'a_petrenko_9_ram_location';")
                if cur.fetchone()[0] == 0:
                    cur.execute('CREATE TABLE public.a_petrenko_9_ram_location(id text,name text,type text,'
                                'dimension text,resident_cnt text)', False)
                    cur.commit()
        conn.close()

    def load_csv_to_gp_func(self):
        pg_hook = PostgresHook('conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"truncate a_petrenko_9_ram_location;")
        pg_hook.copy_expert("COPY a_petrenko_9_ram_location FROM STDIN DELIMITER ','", '/tmp/a_petrenko_9_ram_toploc'
                                                                                       '.csv')
        conn.close()

    def execute(self, context):

        self.create_table()

        hook = APetrenkoRickMortyHook('dina_ram')
        get_locations_arr = hook.get_locations()

        for pg in range(get_locations_arr):
            page = hook.get_location_pages(str(pg+1))
            for loc_in_page in page:
                self.data_for_pages[loc_in_page.get('id')] = len(loc_in_page.get('residents'))

        top_loc = list(dict(sorted(self.data_for_pages.items(), key=lambda item: item[1], reverse=True)).keys())[:3]

        for_df = []
        for l in hook.get_single_location(top_loc):
            id_tbl = l['id']
            name_tbl = l['name']
            type_tbl = l['type']
            dimension_tbl = l['dimension']
            lst_res_cnt = len(l['residents'])
            for_df.append({'id': str(id_tbl),
                           'name': str(name_tbl),
                           'type': str(type_tbl),
                           'dimension': str(dimension_tbl),
                           'resident_cnt': str(lst_res_cnt)})
        df = pd.DataFrame(for_df, columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        df.to_csv('/tmp/a_petrenko_9_ram_toploc.csv', sep=',', header=False, index=False)

        self.load_csv_to_gp_func()

        logging.info('Finish!')
