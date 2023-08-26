import logging
import collections

import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class GlebBudnikRamSpeciesCountOperator(BaseOperator):
    """
    Information about location and count residents

    Returns: id, name, type, dimension, resident_cnt
    """

    ui_color = "#e0ffff"

    def __init__(
        self,
        num_max: int = 3,
        api_url: str = "https://rickandmortyapi.com/api/location",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.num_max = num_max
        self.api_url = api_url

    def get_count_page(self) -> int:
        r = requests.get(self.api_url)
        if r.ok:
            count_pages = r.json().get("info").get("pages")
            logging.info(f"NUMBER PAGE {count_pages}")
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException("Error in load page count")
        return count_pages

    def resident_in_loc(self, page):
        ram_char_url = f"{self.api_url}/?page={page}"
        r = requests.get(ram_char_url)

        for data_location in r.json().get("results"):
            self.location_info.append(
                (
                    data_location.get("id"),
                    data_location.get("name"),
                    data_location.get("type"),
                    data_location.get("dimension"),
                    len(data_location["residents"]),
                )
            )

    def write_data(self, data: list):
        data = sorted(data, key=lambda x: x[4], reverse=True)[: self.num_max]
        data_write = ", ".join([str(row) for row in data])
        logging.info(data_write)
        return data_write

    def execute(self, context):
        self.location_info = []

        num_pages = self.get_count_page()

        for page in range(1, num_pages + 1):
            logging.info(f"PAGES ITERARION {page}")
            self.resident_in_loc(page)
        logging.info(f"LOCATION INFO {self.location_info}")
        data = self.write_data(self.location_info)
        logging.info(f"RESULT {data}")
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        pg_hook.run(
            f"""
            INSERT INTO g_budunik_ram_location
            VALUES
            {data};
            """
        )
