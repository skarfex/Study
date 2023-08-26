from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
import logging
import requests

class eUvaliev19RamLocationHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations(self) -> list:
        # Уже возвращает результат работы в виде словаря
        return self.run(f'api/location').json()['results']

class eUvaliev19RamTop3LocationOperator(BaseOperator):
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def load_top3_location(self, api_url: str):
        # Загружаем данные с помощью eUvaliev19RamLocationHook
        # Сортируем их и выбираем топ-3
        # Сохраняем в переменную класса
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info('---------------------------------')
            logging.info(f"SUCCESS connection to {api_url}")
            logging.info('---------------------------------')
            # Конвертируем в json, запоминаем только значимую часть ответа
            #.locations = r.json.loads(r.text)['results']
            locations = r.json()['results']
        else:
            logging.warning(f"HTTP STATUS {r.status_code}")
            raise AirflowException('Error in load locations')
        
        # Соберём локации в массив словарей
        location_list = []
        for location in locations:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)

        # Отсортируем этот массив и выберем три первых элемента
        sorted_locations = sorted(location_list,
                                key=lambda cnt: cnt['resident_cnt'],
                                reverse=True)
        top3_location = sorted_locations[:3]
        return top3_location

    def execute(self, context):
        top3_location = self.load_top3_location('https://rickandmortyapi.com/api/location')
        # Соберём список значений для вставки в таблицу
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                        for loc in top3_location]
        # Загружаем данные в Greenplum
        insert_sql = f"INSERT INTO e_uvaliev_19_ram_location VALUES {','.join(insert_values)}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(insert_sql, False)