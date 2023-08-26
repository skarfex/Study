from airflow.hooks.http_hook import HttpHook


class DKairbekov19RAMHook(HttpHook):
    """
    Interact with Rick&Morty API.
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = "GET"

    def get_page_count(self) -> int:
        """Returns count of page in API"""
        return self.run("api/location").json().get("info").get("pages")

    def get_page_data_by_page_id(self, page_id: int):
        """Returns JSON page data from API"""
        return self.run(f"api/location?page={page_id}").json()

    def get_location_count(self) -> int:
        """Returns count of location in API"""
        return self.run("api/location").json().get("info").get("count")

    def get_location_data_by_location_id(self, location_id: int):
        """Returns JSON location data from API"""
        return self.run(f"api/location/{location_id}").json()
