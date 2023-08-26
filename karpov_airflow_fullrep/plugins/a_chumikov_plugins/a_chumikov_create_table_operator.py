from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class CreateTableOperator(BaseOperator):
    """
    Формирует запрос на создание таблицы и посылает его в БД на исполнение
    """

    ui_color = '#DBC9DB'

    def __init__(self, table_name: str, columns_types_dict: dict, **kwargs) -> None:

        super().__init__(**kwargs)

        self.columns_types_list = [f"{k} {v}" for k, v in columns_types_dict.items()]
        self.table_name = table_name

    def make_query_create_table(self):

        return f'create table if not exists {self.table_name} ({", ".join(self.columns_types_list)});'

    def execute(self, context) -> None:

        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run(self.make_query_create_table(), autocommit=True)


