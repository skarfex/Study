from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook


class RagimRickMortyHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_locations(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['results']

    def get_location_page(self, id_location: int) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/{id_location}').json()


class RagimTopLocationsOperator(BaseOperator):
    """
    Top Locations by count residents
    """

    template_fields = ('n_tops',)
    ui_color = "#e0ffff"

    def __init__(self, n_tops: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n_tops = n_tops

    @staticmethod
    def top_ids(locations: list, n: int = 3):
        """
        Returns the id's of the first n_tops by residents locations
        :param locations, count of tops
        :return list ids of top
        """

        ids = []
        index = 0

        for element in locations:
            count = len(element['residents'])
            element['count_residents'] = count

        resident_count = [[el['id'], el['count_residents']]
                          for el in locations]
        counts = [el['count_residents'] for el in locations]

        counts_sorted = counts[:]
        counts_sorted.sort(reverse=True)
        for element in counts_sorted[:n]:
            i = counts.index(element)
            ids.append(resident_count[i][0])
            counts_sorted[index] = -1
            index += 1

        return ids

    def execute(self, **kwargs):
        """
        Return n_tops by residents of locations
        """

        # TODO: Убрать повторный запрос

        hook = RagimRickMortyHook('dina_ram')
        locations_all = hook.get_locations()
        ids = self.top_ids(locations_all, self.n_tops)

        locations = []
        for id in ids:
            location = hook.get_location_page(id)
            locations.append(location)

        return locations
