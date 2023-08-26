
from airflow.providers.http.hooks.http import HttpHook

class ApiHook(HttpHook):
    """
    p-ovchinnikov-20_api_operator.py
    """

    def __init__(self, conn: str, **kwargs) -> None:
        super().__init__(http_conn_id=conn, **kwargs)
        self.method = 'GET'

    def get_count_all_pages(self):
        return self.run('api/location').json()['info']['pages']

    def get_list_locations(self, page_id: int):
        return self.run(f'api/location?page={str(page_id)}').json()['results']