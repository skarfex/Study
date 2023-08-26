import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class MGrigorevaRamLocationsOperator(BaseOperator):
    """
    Return top 3 locations
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Return number of pages from API
        :param api_url
        :return: pages_count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            pages_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {pages_count}')
            return int(pages_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_all_data(self, page_url: str) -> int:
        """
        Get number all data from API
        :return: total_results
        """
        total_results = []

        for page in range(1, self.get_page_count(page_url) + 1):
            r = requests.get(page_url + str(page))
            if r.status_code == 200:
                logging.info(f'PAGE {page}')
                results = r.json()['results']

                for result in results:
                    total_results.append(result)
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load all data from Rick&Morty API')
        return total_results
        logging.info('Total results returned')

    def execute(self, context):
        """
        Return top 3 locations
        :param result_json
        :return: top_3_locations

        """
        result_json = self.get_all_data('https://rickandmortyapi.com/api/location?page=')
        residents_data = []
        for i in range(len(result_json)):
            result = dict(id=result_json[i]['id'],
                          name=result_json[i]['name'],
                          type=result_json[i]['type'],
                          dimension=result_json[i]['dimension'],
                          resident_cnt=len(result_json[i]['residents']))
            residents_data.append(result)

        locations_sorted = sorted(residents_data, key=lambda x: x['resident_cnt'], reverse=True)
        top_3_locations = locations_sorted[:3]
        return top_3_locations
        logging.info('Top 3 locations returned')
        logging.info(top_3_locations)