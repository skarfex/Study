import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator


class VladNeverovskijRickBadMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_top_three_locations(self):
        """Returns TOP 3 locations from API"""
        return sorted([(l['id'], l['name'], l['type'], l['dimension'], len(l['dimension'])) for l in
               self.run('api/location').json()['results']], key=lambda i: i[4])[-3:]


class VladNeverovskijRickBadMortyOperator(BaseOperator):
    """
    Get TOP 3 locations from VladNeverovskijRickBadMortyHook and insert them to GP
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        """
        Makes all...
        """
        http_hook = VladNeverovskijRickBadMortyHook('dina_ram')
        pg_hook = PostgresHook('conn_greenplum_write')
        rows = http_hook.get_top_three_locations()
        logging.info(f'Top 3 locations: {rows}')
        pg_hook.insert_rows(table='v_neverovskij_ram_location', rows=rows,
                            target_fields=('id', 'name', 'type', 'dimension', 'resident_cnt'))