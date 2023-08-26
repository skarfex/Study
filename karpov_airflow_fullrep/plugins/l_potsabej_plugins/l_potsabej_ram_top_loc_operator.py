from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import requests
import logging


class PotsabeyRamTopLocOperator(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_location_count(self, api_url) -> int:
        """
        Get count of loc in API
        :param api_url
        :return: loc count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            location_count = r.json().get('info').get('count')
            logging.info(f'location_count = {location_count}')
            return location_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in get_location_count')

    def get_top_3ram_locations(self, api_url):
        """
        Get id, name, type, dimension, resident_cnt from locations
        :return: list of lists [id, name, type, dimension, resident_cnt] top 3 locations over resident_cnt desc
        """
        locations = []
        cnt = 1

        while cnt <= self.get_location_count(api_url):
            r = requests.get(api_url + '/' + str(cnt))
            if r.status_code == 200:
                loc_id = r.json().get('id')
                name = r.json().get('name')
                loc_type = r.json().get('type')
                dimension = r.json().get('dimension')
                resident_cnt = len(r.json().get('residents'))
                location = [loc_id, name, loc_type, dimension, resident_cnt]
                locations.append(location)
                logging.info(f'location {name} has {resident_cnt} residents')
                cnt += 1
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in proc get_top_3_location')

        top_3ram_locations = sorted(locations, key=lambda i: i[4], reverse=True)[:3]
        logging.info(top_3ram_locations)
        return top_3ram_locations

    def execute(self, context):
        api_url = 'https://rickandmortyapi.com/api/location'
        sorted_location = self.get_top_3ram_locations(api_url)
        return sorted_location
