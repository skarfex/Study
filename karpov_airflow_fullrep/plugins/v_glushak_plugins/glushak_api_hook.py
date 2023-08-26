from airflow.providers.http.hooks.http import HttpHook

class GlushakRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_count_locations(self) -> int:
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['count']

    def get_all_locations(self, count_location: int) -> list:
        """Returns count of page in API"""
        list_id_locations = [i for i in range(1, count_location+1)]
        return self.run(f'api/location/{list_id_locations}').json()