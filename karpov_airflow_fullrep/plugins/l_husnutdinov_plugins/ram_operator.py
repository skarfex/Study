import logging
import requests

from airflow.models import BaseOperator


class RickAndMortyTopLocationsByResidents(BaseOperator):
    """
    Get top N locations based on number of residents
    """
    url = 'https://rickandmortyapi.com/api/location'

    def __init__(self, top_n: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_n = top_n

    def execute(self, context):
        """

        """
        logging.info("Request to API")

        r = requests.get(self.url)
        locations = []

        if r.status_code == 200:
            pages = r.json()['info']['pages']

            for page in range(pages):
                locations += requests.get(f'{self.url}?page={page + 1}').json()['results']

            for location in locations:
                location['residents'] = len(location['residents'])

            locations.sort(key=lambda el: el['residents'], reverse=True)

            return locations[:self.top_n]
