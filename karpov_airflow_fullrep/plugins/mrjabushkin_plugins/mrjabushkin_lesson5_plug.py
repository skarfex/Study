'''
Урок №5 Рябушкин Максим Сергеевич

Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
'''
import requests
import logging
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
import pandas as pd


class MryabOper(BaseOperator):
    ui_color = "#e1ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_resident_count_on_page(self, result_json: str) -> int:
        data=[]
        #print(result_json[0])
        for i in range(len(result_json)):
            res_id=result_json[i]['id']
            name=result_json[i]['name']
            type_=result_json[i]['type']
            dem=result_json[i]['dimension']
            res=len(result_json[i]['residents'])
            data.append([res_id,name,type_,dem,res])
        return data

    def get_page_count(self, api_url):
        r=requests.get(api_url)
        if r.status_code==200:
            logging.info('SUCCESS')
            page_count=r.json().get('info').get('pages')
            logging.info(f'Pages count {page_count}')
            return page_count
        else:
            logging.warning('FATAL: {}', format(r.status_code))
            raise AirflowException('FATAL')

    def execute(self, context):
        data=[]
        url='https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(url.format(pg='1'))):
            r = requests.get(url.format(pg=str(page + 1)))
            if r.status_code == 200:
                data+=self.get_resident_count_on_page(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        sort_df=pd.DataFrame(data, columns=['id','name','type','dimension','residents']).sort_values(by='residents', ascending=False)
        df_ld=sort_df.head(3)
        try:
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
            conn = pg_hook.get_sqlalchemy_engine()
            #conn = create_engine(pg_hook)
            df_ld.to_sql('m-rjabushkin_ram_location',con=conn, if_exists='replace')
            logging.info('SUCCESS')
        except:
            raise AirflowException('FATAL')