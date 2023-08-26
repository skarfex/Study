from airflow.providers.http.hooks.http import HttpHook

class GunichevaApiHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_location_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_char_page(self, page_num) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location?page={page_num}').json()['results']
