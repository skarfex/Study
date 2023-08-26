# С помощью API (https://rickandmortyapi.com/documentation/#location)
# найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

import logging
import requests
import json
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class AFilatova20ramlocatioper (BaseOperator):

    api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    template_fields = ("num_of_locations",)
    ui_color = '#e0ffff'

# по умолчанию задаем поиск по топ-3
    def __init__(self, num_of_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.num_of_locations = num_of_locations

# забрали инф-ию по общему кол-ву страниц
    def num_of_pages(self):
        r = requests.get(self.api_url.format(pg='1'))
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

# собрали инф-ию для таблицы в ГП по каждой странице, посчитали кол-во резидентов
# отсортировали по кол-ву резидентов и отобрали топ-3
# resident_cnt — длина списка в поле residents
    def top_locations(self, num_of_locations):
        result_table = []
        for page in range(self.num_of_pages()):
            r = requests.get(self.api_url.format(pg=str(page+1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                converted_json = json.loads(r.text)['results']
                for column in converted_json:
                    converted_location = {
                        'id': column['id'],
                        'name': column['name'],
                        'type': column['type'],
                        'dimension': column['dimension'],
                        'resident_cnt': len(column['residents'])
                    }
                    result_table.append(converted_location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load info about resident count')
        return sorted(result_table, key=lambda count: count['resident_cnt'], reverse=True)[:num_of_locations]

# используется соединение 'conn_greenplum_write'
    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        result_data = []
        for value in self.top_locations(self.num_of_locations):
            prepared = f"({value['id']}, \'{str(value['name'])}\', \'{str(value['type'])}\', \'{str(value['dimension'])}\', {value['resident_cnt']})"
            # prepared = [value['id'], value['name'], value['type'],
            #             value['dimension'], value['resident_cnt']]
            result_data.append(prepared)

# #очищаем таблицу перед записьмую во избежание дублей
#         sql_query0 = '''TRUNCATE TABLE a_filatova_20_ram_location'''
#         cursor.execute(sql_query0)
#         conn.commit()

# Запишите значения соответствующих полей этих трёх локаций в таблицу.
        sql_query1 = f'''INSERT INTO a_filatova_20_ram_location VALUES {','.join(result_data)}'''
        cursor.execute(sql_query1)
        conn.commit()
