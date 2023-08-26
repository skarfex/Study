import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class ChigrinovaLocationOperator(BaseOperator):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_page_count(self) -> int:
        r = requests.get('https://rickandmortyapi.com/api/location?page=1')
        page_count = r.json().get('info').get('pages')
        return page_count

    def get_one_page_residents_count(self, api_data_entries: list) -> list:
        page_records = []
        for entry in api_data_entries:
            page_records.append([entry.get('id'),
                                 entry.get('name'),
                                 entry.get('type'),
                                 entry.get('dimension'),
                                 len(entry.get('residents'))])
        return page_records

    def get_all_pages_residents_count(self) -> list:
        total_results_unfold = []
        for page in range(self.get_page_count()):
            url_with_page_num = 'https://rickandmortyapi.com/api/location?page=' + str(page + 1)
            r = requests.get(url_with_page_num)
            total_results_unfold.append(self.get_one_page_residents_count(r.json().get('results')))
        total_results = [tuple(item) for sublist in total_results_unfold for item in sublist]
        return total_results

    def get_top_3_locations(self, total_results) -> list:
        return sorted(total_results, key=lambda x: x[4], reverse=True)[:3]

    def execute(self, context):
        all_locations = self.get_all_pages_residents_count()
        top_3_locations = self.get_top_3_locations(all_locations)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        for record in top_3_locations:
            sql_command = f"INSERT INTO a_chigrinova_13_ram_location(id, name, type, dimension, resident_cnt) VALUES {str(record)};"
            pg_hook.run(sql_command)
