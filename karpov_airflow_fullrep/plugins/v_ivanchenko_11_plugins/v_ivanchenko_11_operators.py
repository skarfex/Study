import logging
from requests import get

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

TABLE_NAME_STAGE = 'public."v-ivanchenko-11_ram_location_stage"'


def get_sql_create_table(table_name: str):

    return f"""
        create table if not exists {table_name}(
            id integer,
            name text,
            type text,
            dimension text,
            resident_cnt integer
        );
    """


def get_sql_truncate_table(table_name: str):

    return f'truncate table {table_name}'


class IvanchenkoLoadRamLocationToStage(BaseOperator):
    """
        Оператор осуществляет выгрузку локаций сериала "Рик и Морти" и загружает их в stage слой
        (таблица public."v-ivanchenko-11_ram_location_stage").
        Источник: https://rickandmortyapi.com/api/location
        Такой подход позволяет иметь доступ к достаточно "сырым"(с минимальной обработкой) данным и реализовывать
        основную логику обработки данных на стороне Greenplum.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.base_url = 'https://rickandmortyapi.com/api/location'
        self.table_name = TABLE_NAME_STAGE

    def get_locations_cnt(self) -> int:

        r = get(self.base_url)

        if r.status_code == 200:
            location_cnt = r.json().get('info').get('count')
            logging.info(f'Location_cnt = {location_cnt}')
            return location_cnt
        else:
            logging.warning(f'GET status(location count): {r.status_code}')
            raise AirflowException('Error in load location count!')

    def get_locations(self, location_cnt: int):

        r = get(f'{self.base_url}/{list(range(1, location_cnt + 1))}')

        if r.status_code == 200:
            locations = r.json()
            logging.info(f'Locations, example: {locations[0]}')
            return locations
        else:
            logging.warning(f'GET status(locations): {r.status_code}')
            raise AirflowException('Error in load locations!')

    def insert_locations_to_stage_table(self, locations):

        sql_insert_to_stage_table = f"""
            insert into {self.table_name} values (%s, %s, %s, %s, %s);
        """

        locations_for_insert = [
            (int(loc['id']), loc['name'], loc['type'], loc['dimension'], len(loc['residents']), ) for loc in locations
        ]

        pg_hook = PostgresHook('conn_greenplum_write')

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:

                cur.execute(get_sql_create_table(self.table_name), False)

                cur.execute(get_sql_truncate_table(self.table_name), False)

                cur.executemany(sql_insert_to_stage_table, locations_for_insert)

                cur.close()
        conn.close()

    def execute(self, context):

        locations_cnt = self.get_locations_cnt()

        locations = self.get_locations(locations_cnt)

        self.insert_locations_to_stage_table(locations)


class IvanchenkoLoadRamLocation(BaseOperator):
    """
        Оператор осуществляет загрузку top 3 локации сериала "Рик и Морти" с наибольшим количеством резидентов
        в таблицу public."v-ivanchenko-11_ram_location".
        Источником является stage слой(таблица public."v-ivanchenko-11_ram_location_stage").
        Обработка данных переложена на Greenplum.
        Такой подход позволяет разгрузить Airflow и делает решение достаточно масштабируемым: например, на стороне
        Greenplum можно реализовать процедуры с различной логикой обработки данных и передавать их имена как аргумент.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name_stage = TABLE_NAME_STAGE
        self.table_name_target = 'public."v-ivanchenko-11_ram_location"'

    def insert_locations_to_target_table(self):

        sql_insert_to_target_table = f"""
            insert into {self.table_name_target}
            select * from {self.table_name_stage}
            order by resident_cnt desc limit 3;
        """

        pg_hook = PostgresHook('conn_greenplum_write')

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:

                cur.execute(get_sql_create_table(self.table_name_target), False)

                cur.execute(get_sql_truncate_table(self.table_name_target), False)

                cur.execute(sql_insert_to_target_table, False)

                cur.close()
        conn.close()

    def execute(self, context):

        self.insert_locations_to_target_table()
