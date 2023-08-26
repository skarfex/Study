import json
import logging
import requests

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class NNovikovaRickMortyOperator(BaseOperator):
    api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    #подсчет количества страниц
    def count_pages(self):

        r = requests.get(self.api_url.format(pg='1')) # с помощью функции get обращаемся к переданной в функцию ссылке
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages') # словарь инфо и ключ pages
            logging.info(f'page_count = {page_count}') # запись в лог
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    #Отбираем 3 локации с наибольшим числом резидентов
    def three_top_locations(self, top=3):

        result = []
        for page in range(self.count_pages()):
            r = requests.get(self.api_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                converted_json = json.loads(r.text)['results']
                for location in converted_json:
                    converted_location = {
                        'id': location['id'],
                        'name': location['name'],
                        'type': location['type'],
                        'dimension': location['dimension'],
                        'resident_cnt': len(location['residents'])
                    }
                    result.append(converted_location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return sorted(result, key=lambda count: count['resident_cnt'], reverse=True)[:top]

    #Вставляем данные в таблицу
    def execute(self, context):

        result = []
        #Перебираем массив значений
        for value in self.three_top_locations(top=3):
            i = f"({str(value['id'])}, \'{str(value['name'])}\', \'{str(value['type'])}\'," \
                       f" \'{str(value['dimension'])}\', {str(value['resident_cnt'])})"
            result.append(i)

        # Подключаемся к Гринплам
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        #Создание sql запроса на вставку
        insert_sql = f'''INSERT INTO public.n_novikova_16_ram_location VALUES {','.join(result)}'''
        logging.info(insert_sql)
        pg_hook.run(insert_sql, False)

