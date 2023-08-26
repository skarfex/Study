from airflow.providers.http.hooks.http import HttpHook
class DSibgatovRAMHook(HttpHook):

    def __init__(self, http_conn_id: str, method: str = 'GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_location_pages_count(self) -> int:
        return self.run('api/location').json()['info']['pages']

    def get_page_location(self, page_num:str) -> list:
        return self.run(f'api/location?page={page_num}').json()['results']

