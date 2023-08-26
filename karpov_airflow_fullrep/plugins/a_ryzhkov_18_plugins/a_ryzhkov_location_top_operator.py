import logging
from airflow.models import BaseOperator
import pandas as pd
from sqlalchemy import create_engine
from a_ryzhkov_18_plugins.a_ryzhkov_hook import RyzhkovHook


class RyzhkovFindeTopLocationOperator(BaseOperator):

    def __init__(self, top_position: int = 3, **kwargs):
        super().__init__(**kwargs)
        self.top_position = top_position
        self.dct_to_frame = {'id': [], 'name': [], 'type': [],
                             'dimension': [], 'resident_cnt': []}
        self.conn_postgres = create_engine('postgresql://student:Wrhy96_09iPcreqAS@greenplum.lab.karpov.courses:6432'
                                           '/students')

    def execute(self, **kwargs):
        ram_hook = RyzhkovHook('dina_ram')
        pages_count = ram_hook.count_pages_in_api()

        for i in range(1, pages_count + 1):
            for j in ram_hook.get_next_pages(i):
                self.dct_to_frame['id'].append(j['id'])
                self.dct_to_frame['name'].append(j['name'])
                self.dct_to_frame['type'].append(j['type'])
                self.dct_to_frame['dimension'].append(j['dimension'])
                self.dct_to_frame['resident_cnt'].append(len(j['residents']))
        df = pd.DataFrame(self.dct_to_frame).sort_values(by=['resident_cnt'], ascending=False).head(self.top_position)
        logging.info(f"Фрейм для обновления данных готов: {df.shape}")
        df.to_sql('a_ryzhkov_18_ram_location', con=self.conn_postgres, schema='public', if_exists='append', index=False)