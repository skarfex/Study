# -*- coding: utf-8 -*-

from airflow.providers.http.hooks.http import HttpHook


class ApiHook(HttpHook):
    
    def __init__(self, 
                 http_conn_id: str, 
                 method: str = 'GET', 
                 **kwargs
                 ) -> None:
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_cnt_all_pages(self) -> int:
        pages_cnt = self.run('api/location').json()['info']['pages']
        return pages_cnt

    def get_page(self, page_id) -> list:
        page = self.run(f'api/location?page={page_id}').json()['results']
        return page


