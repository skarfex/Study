
import requests
import logging


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class VLaptevLocationsOperator(BaseOperator):
    """
    Top n locations
    """

    template_fields = ('number_of_locations',)
    ui_color = "#e0ffff"

    def __init__(self, number_of_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.number_of_locations = number_of_locations

    def get_page_count(self, api_url: str) -> int:
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
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        res = []
        ram_locations_url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for page in range(self.get_page_count(ram_locations_url.format(pg='1'))):
            r = requests.get(ram_locations_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')
                locations = r.json().get('results')
                for location in locations:
                    id = location.get("id")
                    name = location.get("name")
                    type = location.get("type")
                    dimension = location.get("dimension")
                    resident_cnt = len(location.get("residents"))
                    res.append({"id": id, "name": name, "type": type, "dimension": dimension, "resident_cnt": resident_cnt})
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        res = sorted(res, key = lambda location: location["resident_cnt"], reverse=True)

        logging.info(f'Top {self.number_of_locations} locations by population in Rick&Morty: {res[:self.number_of_locations]}')
        return res[:self.number_of_locations]