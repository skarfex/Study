from airflow.models.baseoperator import BaseOperator
import logging
from t_egorenkova_18_plugins.t_egorenkova_18_hook import TEgorenkovaHook


class TEgorenkovaOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context, top=3):
        hook = TEgorenkovaHook('dina_ram')
        dicts = []
        if hook.get_status() == 200:
            logging.info(f'HTTP status {hook.get_status()}')
            for location in range(hook.get_number_locations()):
                id = hook.get_location()[location]['id']
                name = hook.get_location()[location]['name']
                type_ = hook.get_location()[location]['type']
                dimension = hook.get_location()[location]['dimension']
                resident_cnt = len(hook.get_location()[location]['residents'])
                _dict = {'id': id, 'name': name, 'type': type_,
                         'dimension': dimension, 'resident_cnt': resident_cnt}
                dicts.append(_dict)
            sorted_dict = sorted(dicts, key=lambda d: d['resident_cnt'], reverse=True)
            sorted_dict = sorted_dict[:top]
            values = []
            for val in sorted_dict:
                value = (val['id'], val['name'], val['type'], val['dimension'], val['resident_cnt'])
                values.append(value)
            return values

