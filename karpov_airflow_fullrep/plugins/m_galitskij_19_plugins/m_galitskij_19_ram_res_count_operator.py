import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

class Mgalitskij19RamResCountOperator (BaseOperator):
    template_fields = ('top_number'),
    ui_color = "#c7fcec"

    def __init__(self, top_number: int = 0, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_number = top_number

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
        
    def get_id_and_res_count_on_page(self, result_json: list) -> dict:
        d = dict()
        for one_char in result_json:
            d[one_char.get('id')] = len(one_char.get('residents'))
        logging.info(f'all in page location {d}')
        return d

    def get_list_task_field(self, result_json: list, id_number: int) -> list:
        my_list = []
        for one_char in result_json:
            if one_char.get('id') == id_number:
                v_id = one_char.get('id')
                my_list.append(v_id)
                v_name = one_char.get('name')
                my_list.append(v_name)
                v_type = one_char.get('type')
                my_list.append(v_type)
                v_dimension = one_char.get('dimension')
                my_list.append(v_dimension)
                v_res_cnt = len(one_char.get('residents'))
                my_list.append(v_res_cnt)
            else:
                logging.info('next id') 
        return my_list
    
    def execute(self, context):
        top_number = self.top_number
        l2 = []       
        d2 = dict()
        sorted_dict = dict()
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg = 1))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                d2.update(self.get_id_and_res_count_on_page(r.json().get('results')))                
                sorted_dict = sorted(
                    d2.items(),
                    key = lambda kv: kv[1],
                    reverse=True)
                l2.extend(self.get_list_task_field(r.json().get('results'),sorted_dict[top_number][0]))                    
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f'CREATE TABLE IF NOT EXISTS m_galitskij_19_ram_location(id int UNIQUE, name varchar, type varchar, dimension varchar, resident_cnt int)')
            logging.info(f'list field all {l2}')
            logging.info(f'INSERT INTO m_galitskij_19_ram_location values({l2[0]},\'{l2[1]}\',\'{l2[2]}\',\'{l2[3]}\',{l2[4]})')   
            cursor.execute(f'INSERT INTO m_galitskij_19_ram_location values({l2[0]},\'{l2[1]}\',\'{l2[2]}\',\'{l2[3]}\',{l2[4]})')
            conn.commit()            
        except Exception:
            logging.info(f'id {l2[0]} is not unique in tables m_galitskij_19_ram_location')
        conn.close()
        logging.info(f'all location {d2}')
        logging.info(f'all sorted location {sorted_dict}')
        logging.info(f'top number is {top_number}')
        logging.info(f'id top number in sorted dict is {sorted_dict[top_number][0]}')
        



                




