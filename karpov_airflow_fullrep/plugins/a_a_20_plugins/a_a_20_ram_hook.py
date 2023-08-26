from airflow.providers.http.hooks.http import HttpHook
import logging
from airflow.models import BaseOperator
import pandas as pd

class aa20_Hook(HttpHook):


    # http_conn_id refers to the Airflow connection ID 
    # configured in the Airflow web interface
    def __init__(self, http_conn_id: str, method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)


    # Get the available number of pages
    def get_location_pages_count(self) -> int:
        return self.run('api/location').json()['info']['pages']


    # Get json information about all locations from the page
    def get_page_locations(self, page_id) -> list:
        return self.run(f'api/location?page={page_id}').json()['results']



class aa20_LocationOperator(BaseOperator):
    # Get the number of residents in each location


    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


    # Parse json with information about locations
    def parse_location_on_page(self, result_json: list) -> list:
        return [
            {"id": query['id'],
            "name": query["name"],
            "type": query["type"],
            "dimension": query["dimension"],
            "resident_cnt": len(query["residents"])} for query in result_json]


    def execute(self, context):
        hook = aa20_Hook("dina_ram")
        list_locations_info = []
        for num_page in range(hook.get_location_pages_count()):
            logging.info(f"PAGE {num_page + 1}")
            json_page_result = hook.get_page_locations(str(num_page + 1))
            list_locations_info += self.parse_location_on_page(json_page_result)
        logging.info("Execute all locations in Rick&Morty")

        data_to_load_df = pd.DataFrame(list_locations_info)
        data_to_load_df.sort_values('resident_cnt', ascending=False, inplace=True)
        values = list(data_to_load_df.itertuples(index=False, name=None))[:3]
        values_to_load = ','.join(map(str, values))
        # values_to_load = [tuple(r) for r in data_to_load_df.head(3).to_numpy()]
        # logging.info(f"Done and ready with {values_to_load}")

        return values_to_load


