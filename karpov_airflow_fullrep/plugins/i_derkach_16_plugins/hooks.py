from airflow.providers.http.hooks.http import HttpHook
from json import loads

class RickMortytHook(HttpHook):
    """ Хук для обращения к API """

    def __init__(self, http_conn_id:str, method:str='GET', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method, self.host = method, 'https://rickandmortyapi.com/'

    def get(self, endpoint:str, method:str='GET') -> dict:
        """ Обращение к API

        >>> ... endpoint ~ (str) - искомая коллекция
        >>> ... method ~ (str) - метод обращения
        >>> return (dict) - полученные данные
        """
        endpoint = endpoint.replace(self.host, '').strip('/')
        try: ### если запрос не отрабатывает или данные некорректны
            response = self.run(endpoint=endpoint)
            if not response.ok: raise Exception(r.text)
            data = loads(response.text)
        except Exception as e:
            raise(e)
        return data
