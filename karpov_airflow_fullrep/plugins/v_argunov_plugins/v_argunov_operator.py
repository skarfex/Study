from v_argunov_plugins.v_argunov_hook import VArgunovHook

import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class VArgunovRamTopLocationOperator(BaseOperator):
    """
    Finds top 'num_locations' locations with greatest number of
    residents
    """
    # template_fields = ('dead_or_alive',) ???
    ui_color = "#c7ffe9"
    def __init__(self, num_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.num_locations = num_locations

    def execute(self, context):
        """
        Finding top n locations and inserting them into sql table
        """
        hook = VArgunovHook(http_conn_id=None)

        locations = hook.get_all_locations()

        top_locations = sorted(locations, key=lambda x: x[4], reverse=True)[:self.num_locations]

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        logging.info("Top 3 locations")
        for location in top_locations:

            sql_query = (f'INSERT INTO students.public.v_argunov_ram_location '
                         f'VALUES ({location[0]}, \'{location[1]}\', '
                         f'\'{location[2]}\', \'{location[3]}\', {location[4]})')
            pg_hook.run(sql_query, False)

            logging.info(f'Location {locations[1]}')
