import requests
import json
import pandas as pd
import logging


from airflow.models import BaseOperator

class LazarevRaMLocationsTop(BaseOperator):
    """
    Counts residents for each location and returns top-3 locations based on residents quantity
    """
    template_fields = ('tops',)
    ui_color = "#e0ffff"

    def __init__(self, tops: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tops = tops

    def pages_count(self, url: str) -> int:
        r = requests.get(url)
        pages = r.json().get('info').get('pages')
        return pages

    def get_location_data(self, result: json):
        result_df = pd.DataFrame()
        for r in result.json().get('results'):
            r_df = pd.DataFrame.from_dict(r)
            result_df = pd.concat([result_df, r_df], ignore_index=True)
        return result_df

    def execute(self, context):
        df = pd.DataFrame()
        locations_url = 'https://rickandmortyapi.com/api/location'
        for page in range(self.pages_count(locations_url)):
            logging.info(f'CURRENT PAGE NUMBER - {page+1}')
            logging.info(f"CURRENT URL IS {locations_url}?page={str(page+1)}")
            data = requests.get(f"{locations_url}?page={str(page+1)}")
            df_append = self.get_location_data(data)
            df_append = df_append.groupby(['id', 'name', 'type', 'dimension'], as_index=False).agg({'residents': 'count'})
            df = pd.concat([df, df_append], ignore_index=True)
        df.sort_values('residents', ascending=False, inplace=True)
        values = list(df.itertuples(index=False, name=None))[:self.tops]
        values_to_load = ','.join(map(str, values))
        logging.info(f"Done and ready with {values_to_load}")
        return values_to_load

