import logging

from airflow.hooks.http_hook import HttpHook


class VArgunovHook(HttpHook):
    """
    Hook for retrieving the location
    """
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_all_locations(self) -> list:
        """
        Get all locations from the RAM API
        """
        data = self.run('https://rickandmortyapi.com/api/location').json()

        all_locations = []
        for location in data['results']:
            data_location = [location['id'],
                             location['name'],
                             location['type'],
                             location['dimension'],
                             len(location['residents'])]

            all_locations.append(data_location)

        return all_locations
