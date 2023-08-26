import json
import logging

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RickAndMortyApiOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_url = 'https://rickandmortyapi.com/api/location?page={pg}'

    def get_page_count(self):
        r = requests.get(self.api_url.format(pg='1'))
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def top3_locations(self, target=3):
        to_return = []
        for page in range(self.get_page_count()):
            r = requests.get(self.api_url.format(pg=str(page+1))).json()
            if requests.get(self.api_url.format(pg=str(page+1))).status_code == 200:
                for location in r["results"]:
                    converted_location = {
                        'id': location['id'],
                        'name': location['name'],
                        'type': location['type'],
                        'dimension': location['dimension'],
                        'resident_cnt': len(location['residents'])
                    }
                    to_return.append(converted_location)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        return sorted(to_return, key=lambda count: count['resident_cnt'], reverse=True)[:target]

    def execute(self):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        with conn.cursor() as cursor:
            prepare_values = []
            for value in self.top3_locations(target=3):
                prepared = f"({value['id']}, '{value['name']}', '{value['type']}'," \
                           f" '{value['dimension']}', {value['resident_cnt']})"
                prepare_values.append(prepared)

            sql_query = f'''INSERT INTO b_matjushin_ram_location(id, name, type, dimension, resident_cnt) 
                            VALUES {','.join(prepare_values)};'''

            cursor.execute(sql_query)
            conn.commit()

            logging.info("Inserted into Greenplum: " + sql_query)


if __name__ == "__main__":
    ex = RickAndMortyApiOperator().top3_locations()
    # ex.top3_locations()