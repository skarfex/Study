import logging
import pandas as pd
from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class LEfremovRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """Returns content of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class LEfremovRamLocationResidentCountOperator(BaseOperator):
    """
    Count number of residents in location
    On L_EfremovRickMortyHook
    """

    #template_fields = ('res',)
    ui_color = "#c7ffe9"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.res = []  # list of dicts with all locations info
        self.data_columns = ['id', 'name', 'type', 'dimension', 'resident_cnt']

    def get_location_info_from_page(self, result_json: list) -> int:
        """
        Get count of dead or alive in one page of character
        :param result_json:
        :return: dead_or_alive_count
        """
        for one_char in result_json:
            location = dict.fromkeys(self.data_columns)
            location['id'] = one_char.get('id')
            location['name'] = one_char.get('name')
            location['type'] = one_char.get('type')
            location['dimension'] = one_char.get('dimension')
            location['resident_cnt'] = len(one_char.get('residents'))
            self.res.append(location)
        logging.info(f'     Location count_on_page = {len(result_json)}')
        return self.res

    def execute(self, **kwargs):
        """
        Logging and execute count of residents in each Rick&Morty location
        """
        hook = LEfremovRickMortyHook('dina_ram')  # some existed connection
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            self.get_location_info_from_page(one_page)

        all_locations = pd.DataFrame(self.res)  # DataFrame from resulted list
        all_locations = all_locations.sort_values(by=['resident_cnt'], ascending=False, ignore_index=True)
        top3 = all_locations.iloc[:3]  # select top 3
        for i, l in top3.iterrows():
            logging.info(f'Top {i+1} location in Rick&Morty is {l["name"]} with {l["resident_cnt"]} residents')
        #kwargs['ti'].xcom_push(value=top3, key='top3_loc')
        return top3
