import logging
import psycopg2
import requests
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class E_Golovenkin_20_Get_Top_Locations(BaseOperator):
    '''
    Get top 3 locations by number of residents from API
    and write data to DB
    '''

    # template_fields = () # It is place for your variables
    ui_colour = '#e0ffff'  # colour of your operator on the interface Airflow

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def top_3_location(self) -> list:
        '''
        Get top 3 locations by number of residents from API
        :param url: 'https://rickandmortyapi.com/api/location'
        :return: max_id_loc (list of id op 3 locations)
        '''
        api_url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(url=api_url)
        if r.status_code == 200:
            data = r.json().get('results')  # выбираем основной блок с данными
            loc_num = len(data)  # находим общее количество локаций
            stat_dict = {}  # пустой словарь для статистики
            x = 0  # инкремент для цикла
            while x != loc_num:
                loc = data[x]
                id = loc.get("id")
                res_cnt = loc.get('residents')
                stat_dict[id] = len(res_cnt)
                x += 1
            logging.info(f'{stat_dict}')  # выводим всю статистику
            max_id_loc = sorted(stat_dict, key=stat_dict.get, reverse=True)[:3]
            logging.info(f'{max_id_loc}')  # топ 3 локации
            return max_id_loc
        else:
            logging.warning(f'HTTP STATUS {r.status_code}')
            raise AirflowException('Error in load page count')

    def execute(self, context):
        '''
        Get top 3 locations by number of residents
        rom API and write data to DB
        '''
        loc_id = self.top_3_location()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        for i in loc_id:
            url_loc = f'https://rickandmortyapi.com/api/location/{i}'
            loc_info = requests.get(url=url_loc).json()
            query = f'''
            insert into e_golovenkin_20_ram_location
            (id, name, type, dimension, resident_cnt)
            values (
                    {loc_info.get('id')}, '{loc_info.get('name')}',
                   '{loc_info.get('type')}', '{loc_info.get('dimension')}',
                    {len(loc_info.get('residents'))}
                    )
                    '''
            logging.info(f'{query}')
            try:
                pg_hook.run(query, False)
            except (psycopg2.Error) as error:
                print("Error while inserting", error)
