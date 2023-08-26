import logging
from airflow.models.baseoperator import BaseOperator


class MPopov9_ram_locations_operator(BaseOperator):


    def __init__(self, count: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.count = count

    def get_page_count(self, api_url):
        import requests
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def page_result(self, page_number):
        import requests
        r = requests.get(f'https://rickandmortyapi.com/api/location/?page={page_number}')
        if r.status_code == 200:
            logging.info("SUCCESS")
            logging.info(f'SUCCESS get_loaction = {page_number}')
            return r.json()['results']
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException(f'Error in load location page {page_number}')

    def execute(self, context):
        from operator import itemgetter
        locations = []
        page_count = self.get_page_count('https://rickandmortyapi.com/api/location')
        for page in range(1, page_count + 1):
            p = self.page_result(page)
            for location in p:
                locations.append((
                    location['id'],
                    location['name'],
                    location['type'],
                    location['dimension'],
                    len(location['residents'])))
        locations.sort(key=itemgetter(-1), reverse=True)
        return locations[:self.count]
