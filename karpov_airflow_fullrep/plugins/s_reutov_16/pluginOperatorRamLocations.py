import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


class SvrRamTopLocation(BaseOperator):

    # template_fields = ('species_type',)

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # self.species_type = species_type

    def get_loc_count(self, api_url: str) -> int:

        r = requests.get(api_url)
        if r.status_code == 200:
            page_count = r.json().get('info').get('count')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load location count')

    def load_ram_func(self, api_url: str) -> list:
        data = {}
        for loc_id in range(self.get_loc_count(api_url)):
            r = requests.get(f"{api_url}/{str(loc_id + 1)}")
            if r.status_code == 200:
                data.update({int(r.json().get('id')): len(r.json().get('residents'))})
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load load_ram_func')
        return list({k: v for k, v in sorted(data.items(), key=lambda item: item[1])})[-3:]

    def execute(self, context):
        ram_char_url = 'https://rickandmortyapi.com/api/location'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"truncate s_reutov_16_ram_location;")
        for loc_id in self.load_ram_func(ram_char_url):
            r = requests.get(f"{ram_char_url}/{str(loc_id)}")
            if r.status_code == 200:
                cursor.execute(f"insert into s_reutov_16_ram_location (id, name, type, dimension, resident_cnt) values "
                               f"({r.json().get('id')},'{r.json().get('name')}','{r.json().get('type')}',"
                               f"'{r.json().get('dimension')}',{len(r.json().get('residents'))});")
                logging.info(f"{r.json().get('id')},{r.json().get('name')},{r.json().get('type')},"
                             f"{r.json().get('dimension')},{len(r.json().get('residents'))}")
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error insert to GP')
