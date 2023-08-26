import pandas as pd
import logging
from airflow.models import BaseOperator
from n_ponomarev.n_ponomarev_ram_hook import PonomarevRickMortyHook


class NPonomarevRamOperator(BaseOperator):
    """
    Находим топ-3 локации с наибольшим количеством резидентов
    """

    template_fields = ('top',)
    ui_color = "#e0ffff"

    def __init__(self, top: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top = top

    def execute(self, context):
        """
        Logging count of concrete species in Rick&Morty
        """
        df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])
        hook = PonomarevRickMortyHook('dina_ram')
        for i in range(1, hook.get_locations_count() + 1):
            k = hook.get_info_of_location(i)
            df = df.append({'id': k.get('id'),
                            'name': k.get('name'),
                            'type': k.get('type'),
                            'dimension': k.get('dimension'),
                            'resident_cnt': len(k.get('residents'))}, ignore_index=True)
            answer = df.sort_values(by='resident_cnt', ascending=False).head(self.top)
        logging.info(answer)
        answer.to_csv('/tmp/n_ponomarev_ram.csv', index=False)
        # Проверка

