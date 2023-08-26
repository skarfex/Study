from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from ang_semenova_plugins.ang_semenova_ram_hook import AngSemenovaRamHook

class AngSemenovaRamOperator(BaseOperator):

    ui_color = '#dcc7ff'


    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    
    def per_page_residents(self, ram_entry: list) -> list:
        pages = []
        for entry in ram_entry:
            pages.append([entry.get('id'),
                         entry.get('name'),
                         entry.get('type'),
                         entry.get('dimension'),
                         len(entry.get('residents'))
                         ])
        return pages

        
    def count_all_residents(self) -> list:

        all_pages = []
        
        ram_hook = AngSemenovaRamHook('dina_ram')
        page_count = ram_hook.get_count_pages()

        for page in range(1, page_count):
            page_locations = ram_hook.per_page_locations(page)
            all_pages.append(self.per_page_residents(page_locations))
        
        result = [tuple(item) for sublist in all_pages for item in sublist]
        return result           


    def top_locations_func(self, result) -> list:
        return sorted(result, key=lambda x: x[4], reverse=True)[:3]


    def execute(self,context):
        locations = self.count_all_residents()
        top_locations = self.top_locations_func(locations)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        
        for value in top_locations:
            sql_command = f"INSERT INTO ang_semenova_ram_location(id, name, type, dimension, resident_cnt) VALUES {str(value)};"
            pg_hook.run(sql_command)
