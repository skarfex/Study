import dataclasses
import heapq
import json
import logging
from typing import Any, Generator
from requests.models import Response

from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    'owner': 'g-tokarev-13',
    'start_date': days_ago(1),
}
TABLE_NAME = 'g_tokarev_13_ram_location'


@dataclasses.dataclass()
class Location:
    id: int
    name: str
    type: str
    dimension: str
    resident_cnt: int

    def __lt__(self, other: 'Location') -> bool:
        return self.resident_cnt < other.resident_cnt

    def __eq__(self, other: 'Location') -> bool:
        return self.resident_cnt == other.resident_cnt

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))

    @staticmethod
    def from_json(json_location):
        return Location(**json.loads(json_location))


class RaMMostPopulatedLocationsOperator(BaseOperator):
    def __init__(self, n_largest: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.n_largest = n_largest

    def execute(self, context: Any):
        heap = []
        for location in self.get_locations():
            heapq.heappush(heap, location)
        top_largest = list(
            map(
                lambda loc: loc.to_json(),
                heapq.nlargest(self.n_largest, heap)
            )
        )
        logging.info(
            f'Top {self.n_largest} most populated location are: {top_largest}'
        )
        context['ti'].xcom_push(
            key='most_populated_locations',
            value=top_largest
        )

    def get_locations(self) -> Generator[Location, None, None]:
        for page in RaMHook().get_pages():
            yield from self.get_locations_on_page(page)

    @staticmethod
    def get_locations_on_page(
            page: Response
    ) -> Generator[Location, None, None]:
        for item in page.json()['results']:
            yield Location(
                item.get('id'),
                item.get('name'),
                item.get('type'),
                item.get('dimension'),
                len(item.get('residents')),
            )


class RaMHook(HttpHook):
    def __init__(
            self,
            http_conn_id: str = 'dina_ram',
            **kwargs
    ) -> None:
        super().__init__(method='GET', http_conn_id=http_conn_id, **kwargs)

    def get_pages(self) -> Generator[Response, None, None]:
        page_count = self.get_page_count()
        for page_number in range(1, page_count + 1):
            yield self.get_page(page_number)

    def get_page_count(self) -> int:
        return self.run('api/location').json().get('info').get('pages')

    def get_page(self, page_number: int) -> Response:
        return self.run(f'api/location?page={page_number}')


with DAG(
    dag_id='g-tokarev-13_ram',
    schedule_interval=None,
    tags=['g-tokarev-13'],
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
) as dag:
    get_most_populated_locations = RaMMostPopulatedLocationsOperator(
        task_id='get_most_populated_locations',
    )

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY, 
                name VARCHAR, 
                type VARCHAR, 
                dimension VARCHAR, 
                resident_cnt INTEGER
            );
        """,
    )

    get_most_populated_locations >> create_table_if_not_exists

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f'TRUNCATE TABLE {TABLE_NAME};',
    )

    create_table_if_not_exists >> truncate_table

    def merge_locations_func(**kwargs):
        most_populated_locations = list(
            map(
                lambda location_json: Location.from_json(location_json),
                kwargs['ti'].xcom_pull(
                    task_ids='get_most_populated_locations',
                    key='most_populated_locations'
                )
            )
        )
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.insert_rows(
            table=TABLE_NAME,
            rows=list(map(dataclasses.astuple, most_populated_locations)),
            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt'],
        )

    merge_locations = PythonOperator(
        task_id='merge_locations',
        python_callable=merge_locations_func,
    )

    truncate_table >> merge_locations
