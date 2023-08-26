from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator
import logging
import requests

class KVerchenkoHook(HttpHook):

    def __init__(self, http_conn_id: str, method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_location_pages_count(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_page_location(self, page_num:str) -> list:
        return self.run(f'api/location?page={page_num}').json()['results']

class KVerchenkoRickMortyOperator(BaseOperator):

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
            # raise AirflowException('Error in load page count')

    def api_data(self):
        """
        Logging count of concrete species in Rick&Morty
        """
        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        result_dict = {}
        all_results = {}
        final = []
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code != 200:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                # raise AirflowException('Error in load from Rick&Morty API')
            else:
                logging.info(f'PAGE {page + 1}')
                id_list = r.json().get('results')
                for i in id_list:
                    i['residents'] = len(i['residents'])
                    result_dict[i['id']] = i['residents']
                    all_results[i['id']] = i
        f = {k: v for k, v in sorted(result_dict.items(), key=lambda item: item[1])}
        top_list = list(f)[-3:]
        for i in top_list:
            all_results[i].pop('url')
            all_results[i].pop('created')
            final.append(tuple(all_results[i].values()))
        return final