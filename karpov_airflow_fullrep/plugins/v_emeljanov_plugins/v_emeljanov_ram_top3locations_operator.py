import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class VEmeljanovRamHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_location_page(self, page_num: str) -> list:
        """Returns page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']


class VEmeljanovRamTopLocationsOperator(BaseOperator):
    """
    Collect top locations by number of residents
    On VEmeljanovRamHook
    """

    template_fields = ('top_cnt',)
    ui_color = "#c7efff"

    def __init__(self, top_cnt: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_cnt = top_cnt

    def get_locations_and_residents_cnt_on_page(self, result_json: list) -> list:
        """
        Get locations with count of residents on one page
        :param result_json:
        :return: residents_count_by_loc
        """
        locations = []
        for one_loc in result_json:
            location = [one_loc['id'],
                        one_loc['name'],
                        one_loc['type'],
                        one_loc['dimension'],
                        len(one_loc['residents'])]
            locations.append(location)
            logging.info(f'    location on page = {location}')
        return locations

    def execute(self, context):
        """
        Get top locations by residents count in Rick&Morty
        On VEmeljanovRamHook
        """
        hook = VEmeljanovRamHook('dina_ram')

        # collect locations from all pages
        locations = []
        for page in range(hook.get_location_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_location_page(str(page + 1))
            locations += self.get_locations_and_residents_cnt_on_page(one_page)

        locations.sort(key=lambda x: x[-1], reverse=True)  # sort locations by residents count
        top_rows = locations[:self.top_cnt]
        logging.info(f'TOP {self.top_cnt} LOCATIONS = {top_rows} :')

        return top_rows
