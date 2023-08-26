"""
Создаем таблицу и складываем туда результаты по ТОП-3 локациям
"""

from airflow import DAG
from airflow.utils.dates import days_ago,datetime
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from k_veryutina_plugins.k_veryutina_ram_location_operator import TopLocations

conn_id = 'conn_greenplum_write'
table_nm = 'k_veryutina_ram_location'

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 6),
    'owner': 'krisssver',
    'poke_interval': 600
}

dag = DAG("k-veryutina-rick-and-morty",
          schedule_interval='0 10 * * *',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True,
          tags=['krisssver','lesson5']
          )

create_and_truncate_table = PostgresOperator(
    task_id='create_and_clear_table',
    postgres_conn_id=conn_id,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {table_nm} (
        "id" int, 
        name varchar, 
        type varchar, 
        dimension varchar, 
        resident_cnt int);
    TRUNCATE TABLE {table_nm};
    """,
    dag=dag
)

get_top3_location = TopLocations(
    task_id='get_top3_location',
    dag=dag
)

# def load_df_to_gp(ti):
#     locations_info = ti.xcom_pull(key='return_value', task_ids='get_top3_location')
#     logging.info('Полученный датафрейм:')
#     logging.info(locations_info)
#     pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     # for i in range(len(locations_info)):
#     #     cursor.execute(f"""INSERT INTO {table_nm} (id, name, type, dimension, resident_cnt)
#     #             VALUES (    {locations_info['id'][i]}
#     #                       , '{locations_info['name'][i].replace("'","")}'
#     #                       , '{locations_info['type'][i].replace("'","")}'
#     #                       , '{locations_info['dimension'][i].replace("'","")}'
#     #                       , {locations_info['resident_cnt'][i]});""")
#     #conn.commit()
#     #conn.close()
#     pg_hook.copy_expert("COPY {table_nm} FROM STDIN DELIMITER ','", '/tmp/k_veryutina_ram_locations.csv')
#     logging.info(f'Данные загружены в {table_nm}')

# load_df_to_gp = PythonOperator(
#     task_id='load_df_to_gp',
#     python_callable=load_df_to_gp,
#     dag=dag
# )

remove_ram_csv = BashOperator(
    task_id='remove_ram_csv',
    bash_command=f'rm -f /tmp/k_veryutina_ram_locations.csv',
    dag=dag
)

create_and_truncate_table >> get_top3_location >> remove_ram_csv#>> load_df_to_gp >> remove_ram_csv
