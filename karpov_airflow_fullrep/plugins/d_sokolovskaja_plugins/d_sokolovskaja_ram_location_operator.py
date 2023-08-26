import logging
#import pandas as pd
import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
#import io

class DSokolovskajaRamLocationOperator(BaseOperator):
    """
    Count number of residents in location
    """

    ui_color = "#e0ffff"

    def __init__(self,tbl_name: str = 'dsokolovskaja_ram_location', **kwargs) -> None:
        super().__init__(**kwargs)
        self.tbl_name = tbl_name

    def get_location_row(self, loc_json, columns:list):
        """Get row from location"""
        loc_dict = {}
        for col in columns:
            #logging.info(col)
            if col.endswith('_cnt'):
                loc_dict[col] = len(loc_json.get(col[:-4] + 's'))
            else:
                loc_dict[col] = loc_json.get(col)
        return loc_dict

    def load_to_gp(self, pg_hook, ins_list, columns):
        #buffer = io.StringIO()
        #buffer.write(df.to_csv(index=None, header=None, sep='\t'))
        #buffer.seek(0)
        #cursor_load = conn.cursor("cursor_load")
        #pg_hook.bulk_load(self.tbl_name, buffer)
        ins_headers = 'insert into ' + self.tbl_name + '(' + ','.join(columns) + ') values('
        ins_values = [ins_headers + str(a.values())[13:-2] + ');' for a in ins_list[:3]]
        sql_query = ' '.join(ins_values)
        logging.info('sql_query: '+sql_query)
        pg_hook.run(sql_query, False)
        logging.info(f'loaded to {self.tbl_name}')

    def prepare_tbl_gp(self, pg_hook):
        #conn = pg_hook.get_conn()
        #cursor_create = conn.cursor("cursor_create")
        #cursor_create.execute(
        pg_hook.run(
                f"""
                create table if not exists {self.tbl_name} (
                id int,
                name text,
                type text,
                dimension text,
                resident_cnt int
                )
                ;
                """
            , False)
        #cursor_create.commit();
        logging.info(f'{self.tbl_name} was created')
        pg_hook.run(f"truncate table {self.tbl_name};",False)
        #cursor_trunc = conn.cursor("cursor_trunc")
        #cursor_trunc.execute(f"truncate table students.{self.tbl_name};")
        #cursor_trunc.commit();
        logging.info(f'{self.tbl_name} was truncated')

    def execute(self, context):
        """
        3 locations with max resident count in Rick&Morty
        """
        columns = ['id', 'name', 'type', 'dimension', 'resident_cnt']
        #df = pd.DataFrame(columns=columns)
        ram_location_url = 'https://rickandmortyapi.com/api/location'
        ram_list = []
        while ram_location_url:
            r = requests.get(url=ram_location_url)
            if r.status_code == 200:
                for one_location in r.json().get('results'):
                    location = self.get_location_row(one_location,columns)
                    #logging.info(location)
                    #df = pd.concat([df, location.to_frame().T], axis=0, ignore_index=True)
                    ram_list.append(location)
                    ram_location_url = r.json().get('info').get('next')
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        #df_max = df.sort_values('resident_cnt', ascending=False)[:3]
        ram_list.sort(key=lambda x: x['resident_cnt'], reverse=True)
        logging.info(f'Locations with max residents in Rick&Morty:')
        logging.info(ram_list[:3])
        logging.info('Start loading to GP')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        #conn = pg_hook.get_conn()  # берём из него соединение
        #cursor = conn.cursor("gp_cursor")  # и именованный (необязательно) курсор
        self.prepare_tbl_gp(pg_hook)  # подготовка таблицы перед обновлением
        self.load_to_gp(pg_hook, ram_list[:3],columns)
        logging.info('Loading locations was finished')




