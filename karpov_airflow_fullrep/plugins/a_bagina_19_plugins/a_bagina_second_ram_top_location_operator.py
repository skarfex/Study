import requests
import logging
import pandas as pd


from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class ABaginaSecondRamTopLocationOperator(BaseOperator):
    """
    Count number of dead concrete species
    """

    ui_color = "d1e231"

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self, api_url: str) -> int:
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get("info").get("pages")
            logging.info(f"page_count = {page_count}")
            return int(page_count)
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException("Error in load page count")

    def get_location_on_page(self, result_json: list, location: list) -> list:
        """
        Get data location in one page
        :param result_json:
        :return: list of data location
        """
        for one_char in result_json:
            location.append(
                [
                    one_char.get("id"),
                    one_char.get("name"),
                    one_char.get("type"),
                    one_char.get("dimension"),
                    len(one_char.get("residents")),
                ]
            )
        return location

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        ram_char_url = "https://rickandmortyapi.com/api/location?page={pg}"
        location = []
        for page in range(self.get_page_count(ram_char_url.format(pg="1"))):
            one_page = requests.get(ram_char_url.format(pg=str(page + 1)))
            if one_page.status_code == 200:
                logging.info(f"PAGE {page + 1}")
                location = self.get_location_on_page(
                    one_page.json().get("results"), location
                )
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException("Error in load from Rick&Morty API")

        df_location = pd.DataFrame(
            location, columns=["id", "name", "type", "dimension", "resident_cnt"]
        )
        df_location = df_location.sort_values("resident_cnt", ascending=False).head(3)
        return df_location
