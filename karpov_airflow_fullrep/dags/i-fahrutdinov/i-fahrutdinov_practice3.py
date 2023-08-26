"""
Даг для практики 2
"""
import pendulum
from airflow import DAG
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {
    'owner': 'i-fahrutdinov',
}


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


def get_info_from_page(url):
    """
    get residents from location
    """
    loc_info = dict()
    json = requests.get(url).json()
    for res in json['results']:
        loc_data = (res['id'], res['name'], res['type'], res['dimension'], len(res['residents']))
        loc_info[res['id']] = loc_data
    return loc_info


def load_ram_func(api_url):
    """
    Logging count of Human in Rick&Morty
    """
    locs = dict()
    ram_char_url = api_url
    for page in range(get_page_count(ram_char_url.format(pg='1'))):
        r = requests.get(ram_char_url.format(pg=str(page + 1)))
        if r.status_code == 200:
            logging.info(f'PAGE {page + 1}')
            url = ram_char_url.format(pg=str(page + 1))
            res = get_info_from_page(url)
            locs.update(res)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')
    return locs


with DAG("i-fahrutdinov_practice3",
         start_date=pendulum.datetime(2022, 11, 14, tz='UTC'),
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-fahrutdinov'],
         ) as dag:

    dummy = DummyOperator(task_id="start")

    def going_api(url, **context):
        all_locs = load_ram_func(url)
        top_locs = sorted(all_locs, key=lambda x: all_locs[x][4])[-3:]
        context["task_instance"].xcom_push(key="top_locs", value=[all_locs[k] for k in top_locs])


    def going_gp(**context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        logging.info('connection made')
        c = conn.cursor()
        records = context["task_instance"].xcom_pull(task_ids="getting_residents", key="top_locs")
        c.execute('drop table if exists i_fahrutdinov_ram_location;')
        logging.info('table dropped')
        c.execute('create table i_fahrutdinov_ram_location '
                  '(id int, name text, type text, dimension text, resident_cnt int);')
        logging.info('table created')
        for rec in records:
            c.execute(f"INSERT INTO i_fahrutdinov_ram_location VALUES "
                      f"({rec[0]}, '{rec[1]}', '{rec[2]}', '{rec[3]}', {rec[4]}) ;")
        conn.commit()
        c.close()


    getting_residents = PythonOperator(
        task_id='getting_residents',
        python_callable=going_api,
        op_kwargs={'url': 'https://rickandmortyapi.com/api/location/?page={pg}'},
        dag=dag
    )

    insert_locations = PythonOperator(
        task_id='insert_locations',
        python_callable=going_gp,
        dag=dag
    )

    dummy >> getting_residents >> insert_locations
