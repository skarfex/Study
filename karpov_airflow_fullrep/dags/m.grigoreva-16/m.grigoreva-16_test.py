from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
import requests
import logging

url = 'https://rickandmortyapi.com/api/location'
page = 'https://rickandmortyapi.com/api/location?page='

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm.grigoreva-16',
    'poke_interval': 600
}

with DAG("m.grigoreva-16_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m.grigoreva-16']
         ) as dag:

    start = DummyOperator(task_id='start')


    def get_page_count(api_url):
        """
        Return number of pages from API
        :param api_url
        :return: pages_count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            pages_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {pages_count}')
            return pages_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def get_all_data(page_url):
        """
        Get number all data from API
        :return:  total_results
        """
        total_results = []

        for page in range(1, get_page_count(page_url) + 1):
            r = requests.get(page_url + str(page))
            if r.status_code == 200:
                logging.info(f'PAGE {page}')
                results = r.json()['results']

                for result in results:
                    total_results.append(result)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load all data from Rick&Morty API')
        logging.info(total_results)
        return total_results

    def return_top_3_locations():
        """
        Return top 3 locations
        :param result_json
        :return: top_3_locations

        """
        result_json = get_all_data('https://rickandmortyapi.com/api/location?page=')
        residents_data = []
        for i in range(len(result_json)):
            result = dict(id=result_json[i]['id'],
                          name=result_json[i]['name'],
                          type=result_json[i]['type'],
                          dimension=result_json[i]['dimension'],
                          resident_cnt=len(result_json[i]['residents']))
            residents_data.append(result)

        locations_sorted = sorted(residents_data, key=lambda x: x['resident_cnt'], reverse=True)
        top_3_locations = locations_sorted[:3]
        return top_3_locations
        logging.info('Top 3 locations returned')
        logging.info(top_3_locations)

    def insert_data_to_db(task_instance):
        data = task_instance.xcom_pull(task_ids='print_top_3_locations')
        data_formated = [f"({x['id']}, '{x['name']}', '{x['type']}', '{x['dimension']}', {x['resident_cnt']})" for x in data]
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        creating_table = """
                    CREATE TABLE IF NOT EXISTS m_grigoreva_16_ram_location (
                            id SERIAL4 PRIMARY KEY,
                            name VARCHAR NOT NULL,
                            type VARCHAR NOT NULL,
                            dimension VARCHAR NOT NULL,
                            resident_cnt INT4 NOT NULL)
                """
        truncating_table = "TRUNCATE TABLE m_grigoreva_16_ram_location"
        inserting_values = f"INSERT INTO m_grigoreva_16_ram_location VALUES {','.join(data_formated)}"

        pg_hook.run(creating_table, False)
        pg_hook.run(truncating_table, False)
        pg_hook.run(inserting_values, False)



    print_pages_count = PythonOperator(
        task_id='print_pages_count',
        python_callable=get_page_count,
        op_args=[url]
    )

    print_results = PythonOperator(
        task_id='print_results',
        python_callable=get_all_data,
        op_args=[page]
    )

    print_top_3_locations = PythonOperator(
        task_id='print_top_3_locations',
        python_callable=return_top_3_locations
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_to_db
    )

    start >> print_pages_count >> print_results >> print_top_3_locations >> insert_data

