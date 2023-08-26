import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook

full_url = 'https://rickandmortyapi.com/api/location'
location_url = 'api/location'

class RickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_page_count(self) -> str:
        return self.run(location_url).json()['info']['pages']

    def get_page(self, page_num: str) -> list:
        return self.run(f'{location_url}/?page={page_num}').json()['results']


class LocationResidentsCountOperator(BaseOperator):
    """
    Count number of residents in location
    on RickMortyHook
    """
    template_fields = ('top_length',)
    ui_color = '#d5a6bd'
    ui_fgcolor = '#701642'

    def __init__(self, top_length: int = 3, endpoint_url: str = '', **kwargs):
        super().__init__(**kwargs)
        self.top_length = top_length
        self.endpoint_url = endpoint_url

    def residents_count_on_location(self, result_json: list) -> int:
        """
        Get count of residents in one location
        and location info: id, name, type, dimension
        param: result_json
        return: dead_or_alive_count
        """
        locations = []
        for result in result_json:
            location = {
                'id': result.get('id'),
                'name': result.get('name'),
                'type': result.get('type'),
                'dimension': result.get('dimension'),
                'resident_cnt' : len(result.get('residents'))
            }
            locations.append(location)
        return locations

    def get_top_locations(self, locations: list) -> list:
        """
        Return top location from all location
        """
        top_locations = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[0:self.top_length]
        return top_locations

    def execute(self, context) -> list:
        """
        Return and logging top location
        """
        locations = []
        hook = RickMortyHook('dina_ram')
        for page in range(hook.get_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_page(str(page + 1))
            locations = locations + self.residents_count_on_location(one_page)
        top_locations = self.get_top_locations(locations)
        logging.info('-----------------------------------------------')
        logging.info(f'Топ {self.top_length} локаций: {top_locations}')
        logging.info('-----------------------------------------------')
        return top_locations

# r = requests.get('https://rickandmortyapi.com/api/location')
# pages = r.json().get('info').get('pages')
# locations = []
# for page in range(pages):
#     r = requests.get(f'https://rickandmortyapi.com/api/location/?page={page+1}')
#     result_json = r.json().get('results')
#     for result in result_json:
#         location = {
#             'id': result.get('id'),
#             'name': result.get('name'),
#             'type': result.get('type'),
#             'dimension': result.get('dimension'),
#             'resident_cnt' : len(result.get('residents'))
#         }
#         locations.append(location)
# top_locations = sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[0:3]
# print(top_locations)