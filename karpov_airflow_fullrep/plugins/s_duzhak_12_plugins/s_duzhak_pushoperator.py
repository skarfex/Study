import json

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class SergDuzhPushOperator(BaseOperator):
    template_fields = ['data']

    def __init__(self, data, **kwargs):
        super().__init__(**kwargs)
        self.data = data  # self.xcom_pull('get_data')

    def execute(self, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        values = []
        print(self.data)
        for el in json.loads(self.data):
            elems = list(el.values())
            str_elems = [f"'{str(e)}'" for e in elems]
            values.append(f"({','.join(str_elems)})")

        sql_com = f"""
        INSERT INTO "s-duzhak-12_ram_location" (id, name, type, dimension, resident_cnt)
        VALUES {','.join(values)}
        """
        pg_hook.run("""
            DELETE FROM "s-duzhak-12_ram_location"
        """, False)
        pg_hook.run(sql_com, False)
