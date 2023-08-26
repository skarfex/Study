import requests
import json
import logging
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class VKorolkoTop3LocationOperator(BaseOperator):

    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def load_top3_location(self):
        # Load data from http
        url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(url)
        if r.status_code == 200:
            data = json.loads(r.text)
            locations = data['results']
            result = []
            for location in locations:
                location_dict = {
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents'])
                }
                result.append(location_dict)

            # Sort data and chose first three elements
            sorted_locations = sorted(result,
                                      key=lambda cnt: cnt['resident_cnt'],
                                      reverse=True)
            self.top3_location = sorted_locations[:3]
        else:
            logging.warning("Error {}".format(r.status_code))
        # Сортируем их и выбираем топ-3
        # Сохраняем в переменную класса

    def execute(self, context):
        self.load_top3_location()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

	    # Collect insert statement
        insert_data=[]
        for loc in self.top3_location:
            insert_values = f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
            insert_data.append(insert_values)

         # load data into Greenplum
        insert_sql = f"INSERT INTO v_korolko_ram_location VALUES {','.join(insert_data)}"
        cursor.execute(insert_sql)
        logging.info('SQL INSERT QUERY: ' + insert_sql)
        conn.commit()
        conn.close()