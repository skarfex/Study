"""
Igor Batyukov

My first custom Airflow hook

"""

from airflow.hooks.http_hook import HttpHook

class IBatjukov10RamLocationsHook(HttpHook):
    """
    Interact with Rick&Morty API / chapter 'locations'
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_page_count(self) -> int:
        """ Returns total numer of pages in API """
        return self.run('api/location').json()['info']['pages']

    def get_locations_on_page(self, page_num: str) -> list:
        """ Returns locations list on particular page """
        return self.run(f'api/location?page={page_num}').json()['results']

    def get_final_json(self, loc_id: str) -> dict:
        """ Returns details of particular location"""
        return self.run(f'api/location/{loc_id}').json()