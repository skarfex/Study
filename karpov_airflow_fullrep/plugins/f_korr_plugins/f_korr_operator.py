import logging
from airflow.models import BaseOperator
from f_korr_plugins.f_korr_ram_hook import KorrRamHook


class KorrRaMTopLocationsOperator(BaseOperator):
    ui_color = "#5A1D75"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        hook = KorrRamHook('dina_ram')
        page_count = hook.get_location_pages_count()
        locations = []

        for page_id in range(1, page_count + 1):
            row = hook.get_page_locations(page_id)
            for i in range(0, len(row)):
                rows = (
                    row[i]['id'],
                    row[i]['name'],
                    row[i]['type'],
                    row[i]['dimension'],
                    len(row[i]['residents'])
                )
                locations.append(rows)

        top_three_locations = ','.join(map(str, sorted(locations, key=lambda x: x[-1], reverse=True)[:3]))
        logging.info(f"Top 3 locations in RaM are {top_three_locations}")
        return top_three_locations
