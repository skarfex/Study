from airflow.models import BaseOperator
import requests
import logging


class AMuromtsev12Top3Locations(BaseOperator):
    """
    Gets id, name, type, dimension, resident_cnt from top n locations of Rick and Morty
    """

    ui_color = "e0ffff"

    def __init__(self, top_n_locations=3, **kwargs):
        super().__init__(**kwargs)
        self.top_n_locations = top_n_locations

    def get_number_of_locations(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            logging.info("GOT number of locations")
            locations_count = r.json().get('info').get('pages')
            logging.info(f'locations count : {locations_count}')
            return locations_count

    def get_number_of_residents_in_location(self, location_number, url):
        r = requests.get(url)
        if r.status_code == 200:
            logging.info("GOT data for residents count")
            residents_count = len(r.json().get('results')[0].get('residents'))
            logging.info(f'location {location_number} has {residents_count} residents')
            return residents_count

    def execute(self, context, **kwargs):
        top_n_counts = self.top_n_locations * [0]
        top_n_locations = self.top_n_locations * [0]
        url = 'https://rickandmortyapi.com/api/location?page={pg}'
        for location in range(1, self.get_number_of_locations(url.format(pg='1')) + 1):
            resident_count = self.get_number_of_residents_in_location(location, url.format(pg=str(location)))
            if resident_count > top_n_counts[0]:
                top_n_counts[0] = resident_count
                top_n_locations[0] = location
                top_n_counts, top_n_locations = (list(t) for t in zip(*sorted(zip(top_n_counts, top_n_locations),
                                                                              key=lambda x: x[0])))
        return top_n_locations
