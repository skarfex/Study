class PonomarevRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations_count(self):
        """Returns count of locations in API"""
        return self.run('api/location').json()['info']['count']

    def get_info_of_location(self, id_of_location: int):
        """Returns list of residents of location"""
        return self.run(f'api/location/{id_of_location}').json()
