from airflow.hooks.http_hook import HttpHook


class MatjushinaTopLocationsHook(HttpHook):
    """
    Manipulating with Rick and Morty API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_location_count(self):
        """ Returns count of page in API """
        return self.run("api/location").json()["info"]["count"]

    def get_info_of_location(self, location_id: int):
        """ Returns list of residents in location """
        return self.run(f"api/location/{location_id}").json()
