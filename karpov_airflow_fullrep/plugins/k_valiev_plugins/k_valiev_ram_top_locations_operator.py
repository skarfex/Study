import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

class ValievRamTopLocationsOperator(BaseOperator): #наследуем от BaseOperator
    """
    Найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов в файле Рик и Морти
    """
    template_fields = ('conn_id', 'table_name',) #переменные, которые будут присваиваться в операторе DAG

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table_name = table_name


    #количество страниц
    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """

        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'Количество страниц = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    #json results
    def get_jsons(self):
        api_url = 'https://rickandmortyapi.com/api/location'
        page_count = self.get_page_count(api_url)

        all_results_json = []
        for i in range(1, page_count):
            url = f'https://rickandmortyapi.com/api/location?page={i}'
            r = requests.get(url)
            if r.status_code == 200:
                logging.info("SUCESS")
                data_json = r.json()
                all_results_json.extend(data_json['results'])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in get top 3 locations')
        return all_results_json


    def get_locations(self):
        all_results_json = self.get_jsons()
        sorted_file_content = sorted(all_results_json, key=lambda x: len(x['residents']), reverse=True)[:3]

        top3_locations = []
        for i in sorted_file_content[:3]:
            top3_locations.append(
                (i.get('id'), i.get('name'), i.get('type'), i.get('dimension'), len(i.get('residents')),))
        return top3_locations


    def execute(self, context):
        top3_locations = self.get_locations()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_values(cursor, f"INSERT INTO {self.table_name} (id, name, type, dimension, residents_cnt) VALUES %s", top3_locations)
        conn.commit()
        cursor.execute(f'SELECT * FROM {self.table_name}')
        query_res_after_insert = cursor.fetchall()
        logging.info(f'OUTPUT after:\n{query_res_after_insert}')






