# -*- coding: utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from aleksey_ivanov_plugins.de20_m4_t5_ram_hook import DE20M4T5RamHook


class DE20M4T5RamOperator(BaseOperator):
    """
    Return selected number of locations with most residents
    """

    template_fields = ("number_of_locations",)

    def __init__(self, number_of_locations=5, **kwargs):
        super().__init__(**kwargs)
        self.number_of_locations = number_of_locations

    def execute(self, context):
        ram_hook = DE20M4T5RamHook(http_conn_id="dina_ram")
        location_page_count = ram_hook.get_location_page_count()
        location_list = []

        for page_num in range(location_page_count):
            page_locations = ram_hook.get_location_page(page_num + 1)
            for location in page_locations:
                location_list.append(
                    (
                        location["id"],
                        location["name"],
                        location["type"],
                        location["dimension"],
                        len(location["residents"]),
                    )
                )

        sorted_locations = sorted(location_list, key=lambda cnt: cnt[-1], reverse=True)
        top_locations = sorted_locations[: self.number_of_locations]

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")

        for loc in top_locations:
            insert_sql = f"INSERT INTO aleksey_ivanov_ram_location VALUES {str(loc)};"
            pg_hook.run(insert_sql, False)
