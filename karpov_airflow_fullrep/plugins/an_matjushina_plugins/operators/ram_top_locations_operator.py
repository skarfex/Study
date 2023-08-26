import logging

from airflow.models import BaseOperator

from an_matjushina_plugins.hooks.ram_hook import MatjushinaTopLocationsHook


class MatjushinaRaMOperator(BaseOperator):
    """
    Count and write to DB popular locations Rick and Morty
    """

    template_fields = ("top_count",)
    ui_color = "#55aac0"

    def __init__(self, top_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_count = top_count

    def get_count_of_residents(self, location: list) -> int:
        return len(location)

    def execute(self, context) -> dict:
        hook = MatjushinaTopLocationsHook("dina_ram")
        locations = list()
        for location_id in range(1, hook.get_location_count() + 1):
            logging.info(f"LOCATION ID {location_id}")
            location_info = hook.get_info_of_location(location_id)
            row = (
                location_id,
                location_info["name"],
                location_info["type"],
                location_info["dimension"],
                len(location_info["residents"]),
            )
            locations.append(row)
            logging.info(f"Location info: {row}")
        top = sorted(locations, key=lambda a: a[-1], reverse=True)[: self.top_count]
        result = ",".join(map(str, top))
        self.log.info(f"Top {self.top_count} location on residents: {result}")

        return result
