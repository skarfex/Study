import requests
import json
from airflow.models import BaseOperator


class LocOperator(BaseOperator):
    """
    Класс оператора
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """Поиск локаций"""

        r = requests.get('https://rickandmortyapi.com/api/location')
        locs = json.loads(r.text)['results']
        loc_list = []
        for loc in locs:
            loc_dict = {
                'id': loc['id'],
                'name': loc['name'],
                'type': loc['type'],
                'dimension': loc['dimension'],
                'resident_cnt': len(loc['residents'])
            }
            loc_list.append(loc_dict)

        res = sorted(loc_list,
                     key=lambda cnt: cnt['resident_cnt'],
                     reverse=True)
        top3 = res[:3]
        return top3
