"""
First hook
"""
from airflow.providers.http.hooks.http import HttpHook


class RyzhkovHook(HttpHook):

    def __init__(self, http_conn_id: str, method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def count_pages_in_api(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_next_pages(self, page_id) -> list:
        return self.run(f'api/location?page={page_id}').json()['results']