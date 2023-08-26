import logging
import json
import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LocationsGutyrchikOperator(BaseOperator):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


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


    def load_data(self, cnt = 3):
        result = []
        url = "https://rickandmortyapi.com/api/location"
        for i in range(self.get_page_count(api_url=url)):
            r1 = requests.get(url + "/{}".format(i + 1))
            if r1.status_code == 200:
                logging.info(f'PAGE {i + 1}')
                converted_location = {
                    'id': r1.json().get('id'),
                    'name': r1.json().get('name'),
                    'type': r1.json().get('type'),
                    'dimension': r1.json().get('dimension'),
                    'resident_cnt': len(r1.json().get('residents'))
                }
                result.append(converted_location)
        return sorted(result, key=lambda count: count['resident_cnt'], reverse=True)[:3]

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        val = []
        for v in self.load_data(cnt=3):
            p = f"({v['id']},'{str(v['name'])}','{str(v['type'])}','" \
                f" {str(v['dimension'])}', {v['resident_cnt']})"
            val.append(p)
        logging.info(val)
        sql_query = f'''INSERT INTO "d_gutyrchik_16_ram_location" VALUES {','.join(val)}'''
        logging.info(sql_query)
        cursor.execute(sql_query)
        conn.commit()
        logging.info("Inserted into Greenplum: " + sql_query)
