from airflow.providers.http.hooks.http import HttpHook

class AngSemenovaRamHook(HttpHook):

    def __init__(self, http_conn_id: str, method = "GET", **kwargs) -> None:
        super().__init__(http_conn_id = http_conn_id, method = method, **kwargs)
    
    def get_count_pages(self) -> int:
        return self.run('api/location').json()['info']['pages']
    
    def per_page_locations(self, page_id) -> list:
        return self.run(f'api/location?page={page_id}').json()['results']
