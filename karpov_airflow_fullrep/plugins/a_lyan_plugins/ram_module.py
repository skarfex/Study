import logging
from typing import Any, List, Dict
from airflow.models import BaseOperator
import requests
from airflow.exceptions import AirflowException
import pandas as pd


def get_page_count(api_url):
    """
    Get count of page in API
    :param api_url
    :return: page count
    """
    r = requests.get(api_url)
    if r.status_code == 200:
        logging.info("SUCCESS")
        page_count = r.json().get('info').get('pages')
        logging.info(f'page_count = {page_count}')
        return page_count
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load page count')


def aggregate_residents(results: List[Any]) -> List[Dict[str, Any]]:
    """
    Get count of page in API
    :param api_url
    :return: page count
    """

    residents_lst = [{"id": query['id'],
                      "name": query["name"],
                      "type": query["type"],
                      "dimension": query["dimension"],
                      "resident_cnt": len(query["residents"])} for query in results]

    return residents_lst


class CountNumberResidentsOperator(BaseOperator):
    """Count number of species on R&M episodes."""

    template_fields = ('k',)
    ui_color = "#e0ffff"

    def __init__(self, k: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.k = k

    def execute(self, context):

        ram_char_url = 'https://rickandmortyapi.com/api/location?page={pg}'

        page_count = get_page_count(ram_char_url.format(pg='1'))

        final_lst = []

        for page in range(page_count):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                logging.info(f'PAGE {page + 1}')

                # aggregate
                final_lst = final_lst + aggregate_residents(r.json().get('results'))
            else:
                logging.warning("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')

        data_to_load_df = pd.DataFrame(final_lst)

        data_to_load_df.sort_values('resident_cnt', ascending=False, inplace=True)

        values = list(data_to_load_df.itertuples(index=False, name=None))[:self.k]
        values_to_load = ','.join(map(str, values))
        logging.info(f"Done and ready with {values_to_load}")

        return values_to_load
