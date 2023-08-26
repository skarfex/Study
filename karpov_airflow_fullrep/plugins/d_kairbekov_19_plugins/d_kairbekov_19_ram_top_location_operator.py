import logging

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook

class DKairbekov19RAMHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_location_count(self) -> int:
        """Returns count of location in API"""
        return self.run("api/location").json().get("info").get("count")

    def get_location_data_by_location_id(self, location_id: int):
        """Returns JSON location data from API"""
        return self.run(f"api/location/{location_id}").json()


class DKairbekov19RAMOperator(BaseOperator):

    ui_color = "#2ECC40"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context) -> list:
        logging.info("Rick&Morty locations list getting -> STARTED")
        hook = DKairbekov19RAMHook("dina_ram")
        locations = []
        for location_id in range(hook.get_location_count()):
            location = hook.get_location_data_by_location_id(location_id)
            locations.append(
                dict(
                    id=location.get("id"),
                    name=location.get("name"),
                    type=location.get("type"),
                    dimension=location.get("dimension"),
                    resident_cnt=len(location.get("dimension"))
                )
            )
            logging.info("{} getted!".format(location.get('name')))
        logging.info("Rick&Morty locations list getting -> SUCCESS")
        return locations
