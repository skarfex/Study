import requests
import logging

from airflow.models import BaseOperator
import pandas as pd
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

class VaBorisovRamOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    template_fields = ('url',)
    ui_color = "#e0ffff"

    def __init__(self, url, **kwargs) -> None:
        super().__init__(**kwargs)
        self.url = url
    
    
    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        def get_top_location(url):
            results = requests.get(url).json()['results']
            df_locations = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
            for location in results:
                df_locations = df_locations.append({
                    'id': location['id'],
                    'name': location['name'],
                    'type': location['type'],
                    'dimension': location['dimension'],
                    'resident_cnt': len(location['residents'])
                }, ignore_index=True)

            df_locations = df_locations.sort_values(by='resident_cnt', ascending=False).set_index('id').head(3)
            logging.info(df_locations)

            return df_locations
        
        def insert_data(df):
            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
            conn = pg_hook.get_conn()  # берём из него соединение
            cursor = conn.cursor()  # и именованный (необязательно) курсор
            
            df.to_sql('va_borisov_ram_location', pg_hook.get_sqlalchemy_engine(), if_exists='replace')
            logging.info('insert succes')


        df = get_top_location(self.url)
        insert_data(df)