from airflow.providers.http.hooks.http import HttpHook


class TEgorenkovaHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_number_locations(self) -> int:
        return len(self.run('api/location').json()['results'])

    def get_location(self):
        return self.run('api/location').json()['results']

    def get_status(self):
        return self.run('api/location').status_code

