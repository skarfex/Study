"""
Загружаем данные из API Рика и Морти
"""

"""
Тестовый даг Кабанова К. 4
Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" 
	 с полями id, name, type, dimension, resident_cnt.
     С помощью API (https://rickandmortyapi.com/documentation/#location) 
	 найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
     Запишите значения соответствующих полей этих трёх локаций в таблицу. 
	 resident_cnt — длина списка в поле residents.
"""
from airflow import DAG
import logging
import requests
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-kabanova-21',
    'poke_interval': 600
}

with DAG("kkabanova_elt3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-kabanova']
) as dag:

    def create_table_func(**kwargs):
        table_name =  str(kwargs['templates_dict']['owner_name']).replace('-','') + '_ram_location'
        request = f"CREATE TABLE if not exists students.{table_name} (id integer, name varchar, type varchar, dimension varchar, resident_cnt integer);"
        print('request' + request)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        pg_hook.run(request, False)
            #.get_conn()
        #cursor = connection.cursor()
        #cursor.execute(request)
        #connection.commit()
        #cursor.close()
        #connection.close()

    def get_page_count(api_url):
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
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_count_on_location(result_json):
        """
        Get count of character in location
        :param result_json
        :return: character count
        """
        for one_location in result_json:
            char_cnt_loc = 0
            for one_char in one_location.get('residents'):
                char_cnt_loc += 1
            str_cnt_loc = {'loc_id': one_location.get('id'),
                           'loc_name': one_location.get('name'),
                           'loc_type': one_location.get('type'),
                           'loc_dimension': one_location.get('dimension'),
                           'loc_resident_cnt': char_cnt_loc}
            logging.info(f'{str_cnt_loc["loc_name"]} = {char_cnt_loc}')
        return str_cnt_loc
    def load_ram_func():
        """
        Logging count character of location in Rick&Morty
        """
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                char_cnt_loc = get_count_on_location(r.json().get('results'))
                #logging.info(f'{char_cnt_loc["loc_name"]} count resident: {char_cnt_loc["loc_resident_cnt"]}')
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')


    start = DummyOperator(task_id='start')

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_func,
        templates_dict={'owner_name': 'k-kabanova-21'},  # {{ task.owner }}
        provide_context=True
    )

    load_ram = PythonOperator(
        task_id='load_ram',
        python_callable=load_ram_func,
        provide_context=True
    )
    end = DummyOperator(task_id='end')

    start >> [create_table, load_ram] >> end
