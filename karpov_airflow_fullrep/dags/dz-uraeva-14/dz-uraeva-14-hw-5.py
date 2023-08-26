import logging
from datetime import datetime
from typing import List, Tuple

import requests
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator

dag = DAG(
    dag_id="dz-uraeva-14-hw-5",
    schedule_interval="15 0 * * *",
    start_date=datetime(2022, 11, 16, 00, 00),
    max_active_runs=1,
    tags=["dz-uraeva-14"]
)

log = logging.getLogger(__name__)


class InsertMaxLocationsOperator(BaseOperator):
    template_fields = ('max_cnt',)
    ui_color = "#c7ffe9"

    def __init__(self, max_cnt: int = 3, url: str = "https://rickandmortyapi.com/api/location", **kwargs) -> None:
        super().__init__(**kwargs)
        self.max_cnt = max_cnt
        self.url = url

    def find_max_cnt_location(self) -> List[Tuple[int, str, str, str, int]]:
        """
        Get list of locations sorted by count of residents in it
        """
        loc_cnt_list = []
        url = self.url
        while url is not None:
            response = requests.get(url)
            if response.status_code == 200:
                res = response.json()
                url = res["info"]["next"]
                for location in res["results"]:
                    loc_cnt_list.append((
                        location["id"],
                        location["name"],
                        location["type"],
                        location["dimension"],
                        len(location["residents"])))
        log.info(f"Got {len(loc_cnt_list)} locations from Rick and Morty API")
        loc_cnt_list.sort(key=lambda locations: -locations[4])
        return loc_cnt_list

    def execute(self, context):
        """
        Inserting N(=max_cnt) rows into Greenplum table
        """
        create_table_sql = """
                CREATE TABLE IF NOT EXISTS public."dz-uraeva-14_ram_location" (
                id int4 NOT NULL,
                name varchar NOT NULL,
                type varchar NOT NULL,
                dimension varchar NOT NULL,
                resident_cnt int4 NOT NULL
                )
                DISTRIBUTED BY (id);
                """
        truncate_table_sql = """ TRUNCATE TABLE public."dz-uraeva-14_ram_location" """
        res_loc_cnt_list = self.find_max_cnt_location()[:self.max_cnt]

        hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        conn = hook.get_conn()
        with conn.cursor() as curs:
            curs.execute(create_table_sql)
            log.info("Created table public.dz-uraeva-14_ram_location")
            curs.execute(truncate_table_sql)
            log.info("Truncated table public.dz-uraeva-14_ram_location")
            curs.executemany("""
                        INSERT INTO public."dz-uraeva-14_ram_location" (id, name, type, dimension, resident_cnt)
                        VALUES (%s, %s, %s, %s, %s)
                        """, res_loc_cnt_list)
            log.info(f"Inserted {len(res_loc_cnt_list)} rows into table public.dz-uraeva-14_ram_location")
            conn.commit()


insert_max_locations = InsertMaxLocationsOperator(
    task_id="insert_max_locations",
    max_cnt=3,
    url="https://rickandmortyapi.com/api/location",
    dag=dag
)
