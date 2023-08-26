from airflow.models.baseoperator import BaseOperator
import requests
import logging

class RnMLocationsOperator(BaseOperator):

    base_url="https://rickandmortyapi.com/api/"
    location_url=base_url+"location/"

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
            raise AirflowException('Error while loading page')

    def __init__(self, top_limit: int, **kwargs) -> None:
        super().__init__(**kwargs)
        # locations limit
        self.top_limit = top_limit

    def execute(self, context):
        """
        Insert top N locations by residents with sql query
        """
        logging.info(f"Gathering {self.top_limit} locations")
        locations = [] # array of records (location, residents count, name, type, dimension)
        location_pages = self.get_page_count(self.location_url)
        for page in range(location_pages):
            r = requests.get(self.location_url + f"?page={str(page + 1)}")
            if r.status_code == 200:
                page_results = r.json().get('results')
                logging.info(f'There are {len(page_results)} locations on {str(page + 1)}')
                for loc in page_results:
                    locations.append((loc.get('id'), len(loc.get('residents')), loc.get('name'), loc.get('type'), loc.get('dimension')))
            else:
                logging.info("HTTP STATUS {}".format(r.status_code))
                raise AirflowException(f'Error loading from Rick Morty API locations page {str(page + 1)}')

        # sort locations array by residents count desc
        locations.sort(key=lambda x: x[1], reverse=True)

        # gather top locations in string for insert query
        values = ''
        for i in range(min(self.top_limit,len(locations))):
            if values != '':
                values = values + ',\n'
            values = values + f"({locations[i][0]},'{locations[i][2]}','{locations[i][3]}','{locations[i][4]}',{locations[i][1]})"

        return values
