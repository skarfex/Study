# -*- coding: utf-8 -*-

from airflow.providers.http.hooks.http import HttpHook


class DE20M4T5RamHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id, **kwargs):
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_location_page_count(self):
        return self.run("api/location").json()["info"]["pages"]

    def get_location_page(self, page_num):
        return self.run(f"api/location?page={page_num}").json()["results"]
