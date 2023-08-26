import requests
import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class KovalevRAMOperator(BaseOperator):
    """
    Достаем из API нужное число строк
    """

    template_fields = ('row_count',)
    ui_color = "#e0ffff"

    def __init__(self, row_count: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.row_count = row_count

    def get_results_from_RAMAPI(self, row_count) -> list:
        """
        Достаем из API нужное число строк
        """
        api_data = requests.get(f"https://rickandmortyapi.com/api/location")
        res = []
        if api_data.status_code == 200:
            logging.info('Connection success')
            for i in api_data.json()['results']:
                res.append([i['id'], i['name'], i['type'], i['dimension'], len(i['residents'])])
            res.sort(key=lambda x: x[4], reverse=True)
            return res[:row_count]
        else:
            logging.warning("HTTP STATUS {}".format(api_data.status_code))
            raise AirflowException('Error in load page count')

    def execute(self, context, **kwargs):
        return self.get_results_from_RAMAPI(self.row_count)
        logging.info(r'results collected')
