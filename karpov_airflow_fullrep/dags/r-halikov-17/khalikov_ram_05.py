"""
Загружаем данные из API Рика и Морти
"""

from collections import defaultdict

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowException

# from khalikov_plugins.khalikov_ram_species_count_operator import DinaRamSpeciesCountOperator
# from khalikov_plugins.khalikov_ram_dead_or_alive_operator import DinaRamDeadOrAliveCountOperator
# from khalikov_plugins.khalikov_random_sensor import DinaRandomSensor


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'khalikov',
    'poke_interval': 600
}

TABLE = 'students.public."r-halikov-17_ram_location"'


with DAG("khalikov_load_ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['khalikov_ram_05']
         ) as dag:

    def create_table():
        logging.info("\t\t\t=============== {} ===============".format("start create table"))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        query_create = (
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE}
            (
                id integer,
                name text,
                type text,
                dimension text,
                resident_cnt integer
            );
            TRUNCATE TABLE {TABLE};            
            """
        )
        pg_hook.run(query_create, False)
        logging.info("\t\t\t=============== {} ===============".format("end create table"))

        return

    cr_tbl = PythonOperator(
        task_id="cr_tbl",
        python_callable=create_table,
        dag=dag
    )


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


    def get_cnt(result_json, ram_dict):
        """
        Get count of human in one page of character
        :param result_json:
        :return: human_count
        """
        for one_char in result_json:
            key = one_char.get("id")
            if key not in ram_dict:
                value = {
                    "id": key,
                    "name": one_char.get("name"),
                    "type": one_char.get("type"),
                    "dimension": one_char.get("dimension"),
                    "resident_cnt": len(one_char.get("residents"))
                }
                ram_dict[key] = value
                logging.info("\t\t\t============ not exist key == {}. value {} ============".format(key, value))
            else:
                value = {
                    "id": key,
                    "name": ram_dict[key]["name"],
                    "type": ram_dict[key]["type"],
                    "dimension": ram_dict[key]["dimension"],
                    "resident_cnt": ram_dict[key]["resident_cnt"] + len(one_char.get("residents"))
                }
                ram_dict[key] = value
                logging.info("\t\t\t============ key exists == {}. value {} ============".format(key, value))

        logging.info("\t\t\t=============== {} ===============".format(ram_dict))
        return


    def get_info_MY():
        ram_dict = {}
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                get_cnt(r.json().get('results'), ram_dict)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        logging.info("\t\t\t=============== {} ===============".format("end get info"))

        return ram_dict

    def write(list_top_3):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        list_values = []
        for value in list_top_3:
            str_val = "("
            str_val += str(value["id"])
            str_val += ", '" + value["name"] + "'"
            str_val += ", '" + value["type"] + "'"
            str_val += ", '" + value["dimension"] + "', "
            str_val += str(value["resident_cnt"])
            str_val += ")"
            list_values.append(str_val)

        query_insert = (
            f"""
            INSERT INTO {TABLE}
            VALUES
                {list_values[0]},
                {list_values[1]},
                {list_values[2]};
            """
        )
        pg_hook.run(query_insert, False)

        return

    def insert_table():
        ram_dict = get_info_MY()

        dict_cnt = defaultdict(int)
        for key, value in ram_dict.items():
            cnt = value["resident_cnt"]
            dict_cnt[key] = cnt
        logging.info(f"\t\t\tdict_cnt == {dict_cnt}")

        sorted_dict = sorted(dict_cnt.items(), key=lambda x:x[1])
        logging.info(f"\t\t\tsorted_dict == {sorted_dict}")

        keys_top_3 = sorted_dict[-3:]
        logging.info(f"keys_top_3 == {keys_top_3}")

        list_top_3 = [ram_dict[key] for key, v in keys_top_3]
        logging.info(f"list_top_3 == {list_top_3}")
        write(list_top_3)

        return


    ins_tbl = PythonOperator(
        task_id="ins_tbl",
        python_callable=insert_table,
        dag=dag
    )


    cr_tbl >> ins_tbl
