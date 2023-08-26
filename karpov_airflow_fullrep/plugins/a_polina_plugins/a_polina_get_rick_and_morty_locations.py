import pandas as pd
import requests
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class APolinaWriteLocationsOperator(BaseOperator):
    """
     top-3 локации из API Рик и Морти по количеству резидентов
    """
    ui_color = "#e0ffff"
    table_name = 'a_polina_ram_location'

    def __init__(self, species_type: str = 'Location', **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url):
        """
        Взять количество страниц в API
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context):
        locations_data = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                result_json = r.json().get('results')
                for one_location in result_json:
                    new_row = {'id': one_location['id'],
                               'name': str(one_location['name']),
                               'type': str(one_location['type']),
                               'dimension': str(one_location['dimension']),
                               'resident_cnt': len(one_location['residents'])}
                    locations_data = locations_data.append(new_row, ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        locations_data = locations_data.sort_values(by='resident_cnt', ascending=False).head(3)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = f'INSERT INTO {APolinaWriteLocationsOperator.table_name} ' \
                        f'(id, name, type, dimension, resident_cnt) VALUES'
        for index in range(locations_data.shape[0]):
            sql_statement += f"\n ({locations_data.iloc[index]['id']}, " \
                             f"'{locations_data.iloc[index]['name']}'," \
                             f" '{locations_data.iloc[index]['type']}', " \
                             f"'{locations_data.iloc[index]['dimension']}'," \
                             f" {locations_data.iloc[index]['resident_cnt']}),"
        sql_statement = sql_statement[:-1] + ';'
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f'DELETE FROM {APolinaWriteLocationsOperator.table_name}')
            cursor.execute(sql_statement)
        conn.close()




