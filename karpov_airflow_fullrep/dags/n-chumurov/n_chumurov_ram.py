import logging
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook


class N_ChumurovRamHook(HttpHook):
    """  Класс для соединения с апи рик и морти  """

    def __init__(self, http_conn_id, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id,  **kwargs)
        self.method = 'GET'

    def get_pages_count(self) -> int: 
        return self.run('api/location').json()['info']['pages']

    def get_page_results(self, page):
        
        return self.run(f'api/location?page={page}').json()['results']

class NChumurovRamTopResidentLocationsOperator(BaseOperator):

    ''' Возвращает топ 3 локации по количеству персонажей '''

    template_fields = ('top')
    ui_color = "#a256c4"

    def __init__(self, top: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top = top

    def execute(self, context):

        hook = N_ChumurovRamHook('dina_ram')
        locations = []

        for page in range(1, hook.get_pages_count() + 1):
            logging.info(f'Page {page}')
            page_result = hook.get_page_results(page)

            for loc in page_result:
                locations.append({

                    'id': loc['id'],
                    'name': loc['name'],
                    'type': loc['type'],
                    'dimension': loc['dimension'],
                    'resident_cnt': len(loc['residents'])

                })
        result =  sorted(locations, key=lambda x: x['resident_cnt'], reverse=True)[:self.top]
        context["ti"].xcom_push(value=result, key="n_chumurov_ram_locations")

