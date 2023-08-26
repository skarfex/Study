import requests
import heapq
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class RamLocationOperator(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pages_records = []
        self.page_count = 0

    def get_page_count(self) -> None:
        r = requests.get('https://rickandmortyapi.com/api/location?page=1')
        page_count = r.json().get('info').get('pages')
        self.page_count = page_count

    @staticmethod
    def get_page_residents_cnt(data: list) -> list:
        one_page_records = []
        for entry in data:
            one_page_records.append([len(entry.get('residents')),
                                     entry.get('id'),
                                     entry.get('name'),
                                     entry.get('type'),
                                     entry.get('dimension'),
                                     ])
        return one_page_records

    def get_all_pages_residents_count(self) -> None:
        all_pages_records = []
        self.get_page_count()
        for page in range(self.page_count):
            url_with_page_num = 'https://rickandmortyapi.com/api/location?page=' + str(page + 1)
            r = requests.get(url_with_page_num)
            page_records = self.get_page_residents_cnt(r.json().get('results'))
            all_pages_records.extend(page_records)
        all_pages_records = [tuple(item) for item in all_pages_records]
        self.pages_records = all_pages_records

    def get_top_k_locations(self, k: int) -> list:
        return heapq.nlargest(k, self.pages_records, key=lambda x: x[0])

    def execute(self, context):
        self.get_all_pages_residents_count()
        top_3_locations = self.get_top_k_locations(3)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        base_query = f'INSERT INTO "r-nigmatullin-19_ram_location"(resident_cnt, id, name, type, dimension)'
        for record in top_3_locations:
            record_query = f' VALUES {str(record)};'
            query = base_query + record_query
            pg_hook.run(query)
