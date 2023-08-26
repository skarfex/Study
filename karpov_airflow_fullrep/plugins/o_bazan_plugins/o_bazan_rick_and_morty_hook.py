'''
Хук OBazanRickMortyHook предназначен для взаимодействия с API Rick&Morty.
Включает два метода:
get_char_page_count - возвращает количество страниц в API
get_char_page - для заданной страницы возвращает список словарей, где каждый словарь содержит информацию о персонаже в API
'''

from airflow.hooks.http_hook import HttpHook

class OBazanRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API
    """
    # Определяем инициализатор. http_conn_id: str означает, что при вызове хука будет ожидаться conn_id
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET' # задаем метод по умолчанию, который будет использоваться при вызове метода run()

    def get_char_page_count(self):
        """
        Get count of pages in API
        :param
        :return count of pages
        """
        return self.run('api/character').json()['info']['pages']

    def get_char_page(self, page_num: str) -> list:
        """
        Get list of characters in API on the page
        :param page
        :return list of characters
        """
        return self.run(f'api/character/?page={page_num}').json()['results']