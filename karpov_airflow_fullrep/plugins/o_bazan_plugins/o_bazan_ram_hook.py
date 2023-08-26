'''
Хук OBazanRAMHook предназначен для взаимодействия с API Rick&Morty.

Включает 3 метода:
get_page_count - возвращает количество страниц в API
get_location_info_on_page - для заданной страницы возвращает список кортежей с информацией о каждой локации на странице
get_location_info - возвращает список кортежей с информацией о каждой локации в API
'''

from airflow.hooks.http_hook import HttpHook

class OBazanRAMHook(HttpHook):
    """
    Interact with Rick&Morty API
    """
    # Определяем инициализатор. http_conn_id: str означает, что при вызове хука будет ожидаться conn_id
    def __init__(self, http_conn_id: str, endpoint: str = 'location', **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.endpoint = endpoint
        self.method = 'GET' # задаем метод по умолчанию, который будет использоваться при вызове метода run()

    def get_page_count(self) -> int:
        """
        Get count of pages in API
        :param
        :return count of pages
        """
        return self.run(f'api/{self.endpoint}').json()['info']['pages']

    def get_location_info_on_page(self, result_json: list) -> list:
        """
        Get info about each location on one page
        :param result_json (list results on one page)
        :return species_count (list of tuples that contain info about each location on one page)
        """
        location_info_on_page = []
        for record in result_json:
            location_info_on_page.append((
                record.get('id'),
                record.get('name'),
                record.get('type'),
                record.get('dimension'),
                len(record.get('residents'))
            ))
        return location_info_on_page

    def get_location_info(self):
        """
        Get info about all locations in API
        :param
        :return species_count (list of tuples that contain info about each location in API)
        """
        location_info = []
        ram_char_api = 'api/{endpoint}/?page={pg}'
        for page in range(self.get_page_count()):
            r = self.run(ram_char_api.format(endpoint=self.endpoint, pg=str(page + 1)))
            location_info.extend(self.get_location_info_on_page(r.json()['results']))
        return location_info
