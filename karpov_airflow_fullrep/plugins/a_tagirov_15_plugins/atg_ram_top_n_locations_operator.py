import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

class AtgRamTopNLocationsOperator(BaseOperator):
    template_fields = ('top_n', 'conn_id', 'table_name',)

    def __init__(self, top_n, conn_id, table_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n
        self.conn_id = conn_id
        self.table_name = table_name
    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_count_residents_on_page(self, result_json: list) -> list:
        """
        Получаем список локаций с описанием и количеством резидентов на каждой из них
        """
        results = []
        for one_char in result_json:
            id = one_char.get('id')
            name = one_char.get('name')
            type = one_char.get('type')
            dimension = one_char.get('dimension')
            resdents = one_char.get('residents')
            resdents_cnt = len(resdents)
            results.append([id, name, type, dimension, resdents_cnt])
        logging.info(f'Locations:\n {results}')
        return results

    def get_list_of_locations_with_residents_cnt(self) -> list:
        """
        Получаем итоговый список со всеми локацями их описанием и количеством резидентов
        """
        results = []
        ram_loc_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(self.get_page_count(ram_loc_url.format(pg='1'))):
            r = requests.get(ram_loc_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                results.append(self.get_count_residents_on_page(r.json().get('results')))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        all_locations = [x for sublist in results for x in sublist]
        logging.info(f'List of all location: {all_locations}')
        return all_locations

    def top_n_location(self, all_locations: list) -> list:
        """
        Получаем список ТОР локаций
        """
        return sorted(all_locations, key=lambda x: x[-1], reverse=True)[:self.top_n]

    def execute(self, context):
        all_locations = self.get_list_of_locations_with_residents_cnt()
        top_location = self.top_n_location(all_locations)
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        #sql_insert = f'INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s'
        execute_values(cursor, f'INSERT INTO {self.table_name} (id, name, type, dimension, resident_cnt) VALUES %s',
                       top_location)
        conn.commit()
        sql_select = (f'SELECT * FROM {self.table_name}')
        cursor.execute(sql_select)
        query_res = cursor.fetchall()
        logging.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        logging.info(query_res)
        logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<')
