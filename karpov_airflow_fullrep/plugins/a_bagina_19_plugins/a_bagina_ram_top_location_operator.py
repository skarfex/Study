import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook


class ABaginaRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_char_page_count(self):
        """Returns count of page in API"""
        return self.run("api/location").json()["info"]["pages"]

    def get_char_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f"api/location?page={page_num}").json()["results"]


class ABaginaRamTopLocationOperator(BaseOperator):
    """
    Get top 3 location by residents count
    On DinaRickMortyHook
    """

    ui_color = "#d8bfd8"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_location_on_page(self, result_json: list, location: list) -> list:
        """
        Get data location in one page
        :param result_json:
        :return: list of data location
        """
        for one_char in result_json:
            location.append(
                [
                    one_char.get("id"),
                    one_char.get("name"),
                    one_char.get("type"),
                    one_char.get("dimension"),
                    len(one_char.get("residents")),
                ]
            )
        return location

    def execute(self, context):
        """
        Get df of location data
        """
        hook = ABaginaRickMortyHook("dina_ram")
        location = []
        for page in range(hook.get_char_page_count()):
            logging.info(f"PAGE {page + 1}")
            one_page = hook.get_char_page(str(page + 1))
            location = self.get_location_on_page(one_page, location)
        logging.info("Execute all locations in Rick&Morty")
        return location
