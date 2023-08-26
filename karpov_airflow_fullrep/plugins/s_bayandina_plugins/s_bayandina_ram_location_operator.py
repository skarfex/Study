import pandas as pd
import requests
from airflow.hooks.postgres_hook import PostgresHook
import logging
from airflow.models.baseoperator import BaseOperator
from airflow import AirflowException

class SBayandinaRamLocationOperator(BaseOperator):

    """
    Load 3 rows with max resident_cnt into DB table
    from 'https://rickandmortyapi.com/api/location'
    """

    # template_fields = ('species_type',)
    ui_color = "#e0ffff"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url):
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

    def find_three_rows_func(self):
        """
        Read all pages in url,
        write all locations in dataframe,
        find 3 rows with max resident_cnt
        """
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        df = pd.DataFrame()
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                for loc in r.json().get('results'):
                    d = {'id': [loc.get('id')], 'name': [loc.get('name')], 'type': [loc.get('type')],
                         'dimension': [loc.get('dimension')], 'resident_cnt': [len(loc.get('residents'))]}
                    df_1 = pd.DataFrame(data=d)
                    df = pd.concat([df, df_1], ignore_index=True)
                    # id_loc = loc.get('id')
                    # df_len=len(df_1)
                    # logging.info(f'Loc with id={id_loc}')
                    # logging.info(f'Length of df_1 ={df_len}')
                df_len = len(df)
                logging.info(f'Length of df ={df_len}')
                logging.info(f'---------------------------')
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        df_count = len(df)
        logging.info(f'Table length: {df_count} rows')
        df = df.sort_values(by=['resident_cnt'], ascending=False).head(3)
        return df

    def execute(self, context):
        """
        Load 3 rows with max resident_cnt into DB table
        from 'https://rickandmortyapi.com/api/location'
        """
        df = self.find_three_rows_func().values.tolist()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        df_len=len(df)
        for i in range(df_len):
            logging.info("Write row №{cnt}".format(cnt=i+1))
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor("named_cursor_name2")  # и именованный (необязательно) курсор
            values = df[i]
            present_id = values[0]
            cursor.execute(f"select exists (select * from s_bayandina_ram_location where id = {present_id}) as table_exists;")  # исполняем sql
            check_flag = cursor.fetchall()[0][0]
            logging.info(f"Flag value is {check_flag}")
            if check_flag:
                text = "Location with id={id} already exists in {table}".format(table='s_bayandina_ram_location', id = present_id)
                logging.info(text)
            else:
                name = "'"+values[1]+"'"
                type_ = "'"+values[2]+"'"
                dimension = "'"+values[3]+"'"
                resident_cnt = values[4]
                pg_hook.run(f"INSERT INTO s_bayandina_ram_location (id, name, type, dimension, resident_cnt) values ({present_id}, {name}, {type_}, {dimension}, {resident_cnt});", False)
                logging.info("Location with id={id} load into 's_bayandina_ram_location'".format(id=present_id))
