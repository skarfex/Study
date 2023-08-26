import requests
from airflow.models import BaseOperator
import logging

class RickMortyLocOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        r = requests.get('https://rickandmortyapi.com/api/location')
        list_locations = r.json().get('results')
        dict_location = {}
        for i in range(0, len(list_locations)):
            temp_dict = list_locations[i]
            temp_dict['residents_cnt'] = len(temp_dict.get('residents'))
            dict_location[f'id_{i}'] = dict(temp_dict)
        cnt_dict = {}
        for i in dict_location.keys():
            cnt_dict[i] = dict_location.get(i).get('residents_cnt')
        top_3 = list((dict(sorted(cnt_dict.items(), key=lambda x: x[1], reverse=True)).keys()))[:3]
        final_dict = {key: dict_location[key] for key in top_3}
        a = final_dict.get('id_19').get('id')
        logging.info(f"{a} sdsdsd")
