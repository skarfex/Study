import logging

from airflow.models.baseoperator import BaseOperator
from d_kairbekov_19_plugins.d_kairbekov_19_ram_hook import DKairbekov19RAMHook


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
