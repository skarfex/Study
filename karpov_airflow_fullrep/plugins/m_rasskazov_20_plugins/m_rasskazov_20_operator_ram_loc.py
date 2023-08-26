import requests
import logging
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import psycopg2
pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
conn = pg_hook.get_sqlalchemy_engine()
engine = create_engine('postgresql://student:Wrhy96_09iPcreqAS@greenplum.lab.karpov.courses:6432/students')

from airflow.models import BaseOperator
class MR20RamLocationCountOperator(BaseOperator):

    def __init__(self, location_top: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.location_top = location_top

    def execute(self, context):
        ram_char_url = 'https://rickandmortyapi.com/api/location'
        r = requests.get(ram_char_url)
        df_l = []
        for i in range(0, len(r.json().get('results'))):
            df_l.append(pd.DataFrame.from_dict(r.json().get('results')[i], orient='index').T)
        df = pd.concat(df_l)
        df['resident_cnt'] = df['residents'].apply(lambda x: len(x))
        sorted_df = df.sort_values('resident_cnt', ascending=False)[['id', 'name', 'type', 'dimension', 'resident_cnt']].head(self.location_top).copy()
        sorted_df.to_sql(name = 'm-rasskazov-20_ram_location', con = engine,index=False, if_exists = 'replace')
        return print(sorted_df)
