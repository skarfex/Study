"""
Урок 3
Сложные пайплайны ч. 1
Задания
"""

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

import requests
import logging
import pandas as pd


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


def get_location(json):
    table_columns = ['id', 'name', 'type', 'dimension', 'residents_cnt']
    page_df = pd.DataFrame(columns=table_columns)
    for location in json:
        location_df = pd.json_normalize(location)
        location_df['residents_cnt'] = location_df['residents'].str.len()
        location_df = location_df.drop(columns=['residents', 'url', 'created'])
        page_df = pd.concat([page_df, location_df], ignore_index=True)
    page_df = page_df.sort_values('residents_cnt', ascending=False).head(3)
    return page_df


def download_data(locations):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    values = []
    for i in range(0, locations['id'].count()):
        row = locations.iloc[i].values.tolist()
        string = "('" + "','".join(str(cell) for cell in row) + "')"
        values.append(string)

    values_insert = ', '.join(str(line) for line in values)
    sql_insert = f'''insert into "e_pavlova_20_ram_location"
            (id, name, type, dimension, residents_cnt) values ''' + values_insert

    logging.info('INSERT QUERY: ' + sql_insert)
    cursor.execute(sql_insert)
    conn.commit()


class top_3_locations(BaseOperator):
    """
    Get top 3 location by the number of residents.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Download date in the table
        """
        columns_lst = ['id', 'name', 'type', 'dimension', 'residents_cnt']
        final_df = pd.DataFrame(columns=columns_lst)
        ram_char_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        for page in range(get_page_count(ram_char_url.format(pg='1'))):
            rg = requests.get(ram_char_url.format(pg=str(page + 1)))
            if rg.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                final_df = pd.concat([final_df, get_location(rg.json().get('results'))], ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        top_list = final_df.sort_values('residents_cnt', ascending=False).head(3)
        download_data(top_list)

