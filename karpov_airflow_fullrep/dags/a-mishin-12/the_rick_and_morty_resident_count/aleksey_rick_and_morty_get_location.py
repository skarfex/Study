"""
DAG работает каждый день, создает таблицу a_mishin_12_ram_location, забирает по API json с локациями
из Рикки и Морти, считает TOP 3 локаций с наибольшем количетсвом резидентов и сохраняет в таблицу a_mishin_12_ram_location
"""

from airflow import DAG
import logging
import requests
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import operator


DEFAULT_ARGS = {
    'owner': 'a-mishin-12',
    'poke_interval': 600,
    'catchup': True,
    'start_date': datetime(2022, 9, 5),
    'retry_delay': timedelta(minutes=1)
}

with DAG("Rick_and_Morty_get_location",
          schedule_interval='0 10 * * *',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mishin-12-lesson-5', 'rick-and-morty']
) as dag:


   # table_name = 'a_mishin_12_ram_location'

    def create_and_empty_table_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql = f"""
                        CREATE TABLE IF NOT EXISTS public.a_mishin_12_ram_location (
                            id int, 
                            name varchar(100), 
                            type varchar(100), 
                            dimension varchar(100), 
                            resident_cnt int);
                        TRUNCATE TABLE public.a_mishin_12_ram_location;
                        """
        cursor.execute(sql)
        logging.info(f'table a_mishin_12_ram_location created and empty')
        conn.commit()
        conn.close()

    """ 1 task"""
    create_and_empty_table = PythonOperator(
        task_id='create_and_empty_table',
        python_callable=create_and_empty_table_func
    )



    def get_3_top_locations(**kwargs):
        """
        Get top location from api_url
        """
        r = requests.get('https://rickandmortyapi.com/api/location')
        if r.status_code == 200:
            logging.info("SUCCESS") #запись в лог, что API вернул ответ и работает

            json_from_api = r.json().get('results')

            for number_location in json_from_api:
                number_location['residents'] = len(number_location['residents'])
                del number_location['url']
                del number_location['created']
            json_from_api.sort(key=operator.itemgetter('residents'), reverse=True)
            top_locations = json_from_api[0:3]
            logging.info(f'page_count = {top_locations}')
            insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['residents']})"
                             for loc in top_locations]
            insert_sql = f"insert into a_mishin_12_ram_location values {','.join(insert_values)}"
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
            pg_hook.run(insert_sql, False)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    """ 2 task"""
    get_3_top_locations = PythonOperator(
        task_id='get_3_top_locations',
        python_callable=get_3_top_locations
    )

    create_and_empty_table >> get_3_top_locations

