
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests
import csv

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-mamleev-7',
    'poke_interval': 600
}


with DAG("i_mamleev_load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-mamleev-7-ram']
         ) as dag:

    start = DummyOperator(task_id='start')


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
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def get_residents_count_on_page(result_json):
        """
        Get count of human in one page of character
        :param result_json:
        :return: human_count
        """
        counter = []
        values = []
        for one_char in result_json:
            id = one_char.get('id')
            name = one_char.get('name')
            type = one_char.get('type')
            dimension = one_char.get('dimension')
            residents_count_on_page = len(one_char.get('residents'))
            logging.info(f'id: {id}, name: {name}, type: {type}, dimension: {dimension}, human_count_on_page = {residents_count_on_page}')
            counter.append(residents_count_on_page)
            values.append((id, name, type,dimension,residents_count_on_page))
        return [residents_count_on_page, dict(zip(counter,values))]


    def load_ram_func():
        """
        Logging count of Human in Rick&Morty
        """
        residents_count = 0
        sorted_locs = {}
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                residents_count += get_residents_count_on_page(r.json().get('results'))[0]
                sorted_locs.update(get_residents_count_on_page(r.json().get('results'))[1])
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        sorted_locs = dict(sorted(sorted_locs.items()))
        logging.info(sorted_locs)
        logging.info(f'Total residents in Rick&Morty: {residents_count}')

        # with open('/tmp/i-mamleev_ram_1.csv', 'w') as csv_file:
        #     writer = csv.writer(csv_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        #     for i in range(1,4):
        #         temp = sorted_locs[list(sorted_locs.keys())[-i]]
        #         writer.writerow(str(temp[0]) + temp[1] + temp[2] + temp[3] + str(temp[4]))
        #         i=+1
        return sorted_locs


    print_human_count = PythonOperator(
        task_id='print_human_count',
        python_callable=load_ram_func
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        create_table_command = f"""
                        CREATE TABLE IF NOT EXISTS i_mamleev_7_ram_location (
                            id text,
                    name text,
                    type text,
                    dimension text,
                    resident_cnt text);
                    """
        pg_hook.run(create_table_command, False)
        sorted_locs = load_ram_func()
        for i in range(1, 4):
            temp = sorted_locs[list(sorted_locs.keys())[-i]]
            values = ["'",str(temp[0]),"','", temp[1],"','", temp[2], "','",temp[3],"','", str(temp[4]),"'"]
            insert_command = f"INSERT INTO i_mamleev_7_ram_location VALUES ({''.join(values)})"
            pg_hook.run(insert_command, False)
            i = +1



    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    start >> load_csv_to_gp #print_human_count >>