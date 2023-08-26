import requests
import logging
import pandas as pd
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook



class top_3_location(BaseOperator):

    def __init__(self, **kwargs):
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
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def top_3(self):
        api_url = 'https://rickandmortyapi.com/api/location/?page={pg}'
        df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        for page in range(self.get_page_count(api_url.format(pg='1'))):
            r = requests.get(api_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                res = requests.get(api_url.format(pg=str(page + 1))).json().get('results')
                l = []
                for i in res:
                    l.append(i)
                df_1 = pd.DataFrame(l)
                df_1 = df_1.rename({'residents': 'resident_cnt'}, axis=1)
                df_1['resident_cnt'] = df_1['resident_cnt'].apply(lambda x: len(x))
                df_1 = df_1.drop(columns=['url', 'created'])
                df = pd.concat([df_1, df], ignore_index=True)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return df.sort_values('resident_cnt', ascending=False).head(3)
    def execute(self, context):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        df = self.top_3()

        val = ["('"+ df.iloc[i].to_string(index=False).replace(' ','')\
                               .replace('\n',"', '") +"')" for i in range(len(df))]
        cursor.execute(f'INSERT INTO "a_mashkarin_6_ram_location" values {",".join(val)}')
        conn.commit()
