"""
    5 урок
    из https://rickandmortyapi.com/api/
    определить 3 лакации с максимальным количеством резедентов
"""

from airflow import DAG

from airflow.utils.dates import days_ago

from s_markevich_plagins.s_markevich_from_api_ram_operator import SMarkevichRamResidentFromLocation
from airflow.operators.postgres_operator import PostgresOperator


DEFAULT_ARGS = {
    'owner': 's-markevich',
    'start_date': days_ago(2),
    'poke_interval': 600,
    'priority_weight': 100,
}


with DAG("s_markevich_lesson_5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['markevich_test'],
         render_template_as_native_obj=True
         ) as dag:

    three_max_resident_cnt = SMarkevichRamResidentFromLocation(
        task_id='three_max_resident_cnt'
    )

    data_to_greenplum = PostgresOperator(
        task_id='data_to_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            insert into s_markevich_ram_location_test 
                {{ ti.xcom_pull(task_ids='three_max_resident_cnt', key='columns') }}
            values
                {{ ti.xcom_pull(task_ids='three_max_resident_cnt', key='values') }}
            ;
        """
    )

    three_max_resident_cnt >> data_to_greenplum
