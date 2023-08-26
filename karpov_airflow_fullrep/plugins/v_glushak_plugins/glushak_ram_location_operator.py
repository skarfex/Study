import logging

from airflow.models import BaseOperator
from v_glushak_plugins.glushak_api_hook import GlushakRickMortyHook
from airflow.hooks.postgres_hook import PostgresHook


class GlushakRamLocationOperator(BaseOperator):
    """
    Количество резидентов на всех локациях Рик и Морти
    """

    ui_color = "#8A2BE2"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def sorted_most_resident_cnt_locations(self, all_locations: list) -> int:
        """
        Количество резидентов на всех локациях
        :param all_locations
        :return: residents_locations_count
        """
        for location in all_locations:
            count_residents = location.get("residents")
            if count_residents is not None:
                location["resident_cnt"] = len(count_residents)
                logging.info(f'id локации {location["id"]} количество резидентов: {location["resident_cnt"]}')
        return sorted(all_locations, key=lambda l: l['resident_cnt'], reverse=True)

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty
        """
        hook = GlushakRickMortyHook('dina_ram')
        count_locations = hook.get_count_locations()
        all_locations = hook.get_all_locations(count_locations)
        sorted_locations = self.sorted_most_resident_cnt_locations(all_locations)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        values = []
        for loc in sorted_locations[:3]:
            esc = ("(",")","'")
            loc['name'] = ''.join(['' if c in esc else c for c in loc['name']])
            values.append(f"({loc['id']}, "
                          f"'{loc['name']}', "
                          f"'{loc['type']}', "
                          f"'{loc['dimension']}', "
                          f"{loc['resident_cnt']})")

        insert = f'''INSERT INTO public.v_glushak_ram_location 
        (id, name, type, dimension, resident_cnt)
         VALUES {",".join(values)}'''
        logging.info(f'Insert: {insert}')
        cursor.execute(insert)
        conn.commit()
