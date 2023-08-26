'''
Hook для получения Python объекта dict после Get запроса к API https://rickandmortyapi.com/api/location'
(чтобы не использовать библиотеку requests для GET-запроса)
'''

from airflow.providers.http.hooks.http import HttpHook


class PveRickMortyHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations(self) -> list:
        # Уже возвращает результат работы в виде словаря
        return self.run(f'https://rickandmortyapi.com/api/location').json()['results']




