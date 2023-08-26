from airflow import DAG
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from l_potsabej_plugins.l_potsabej_ram_top_loc_operator import PotsabeyRamTopLocOperator

DEFAULT_ARGS = {
    'start_date': '2022-12-10',
    'end_date': '2022-12-31',
    'owner': 'l-potsabej',
    'poke_interval': 10
}

with DAG(dag_id="l-potsabej_RaM_DAG",
         schedule_interval='0 0 * * 1-5',
         max_active_runs=1,
         tags=['l-potsabej_ram'],
         default_args=DEFAULT_ARGS
         ) as dag:
    start = DummyOperator(
        task_id='start'
    )

    get_top3_ram_loc = PotsabeyRamTopLocOperator(
        task_id='get_top3_ram_loc'
    )


    def write_loc_to_gp(ti):
        top_3_locations = ti.xcom_pull(key='return_value', task_ids='get_top3_ram_loc')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        with conn.cursor() as cur:
            # создаём таблицу, если нет, чистим значения, если вдруг есть
            cur.execute('''CREATE TABLE IF NOT EXISTS public.l_potsabej_ram_location (id int PRIMARY KEY, name varchar, 
                        type varchar, dimension varchar,resident_cnt int) DISTRIBUTED BY (id); 
                        TRUNCATE TABLE public.l_potsabej_ram_location; 
                        COMMIT;''')
            for loc in top_3_locations:
                # записываем значения каждой локации из топ 3 в таблицу
                cur.execute(f'''INSERT INTO public.l_potsabej_ram_location (id, name, type, dimension, resident_cnt) 
                            VALUES ( {loc[0]}, '{loc[1]}', '{loc[2]}', '{loc[3]}', {loc[4]}); 
                            COMMIT;''')
            conn.close()
            logging.info(f'list of locations uploaded to the database: \n {top_3_locations}')


    write_top3_loc_to_gp = PythonOperator(
        task_id='write_top3_loc_to_gp',
        python_callable=write_loc_to_gp
    )

    end = DummyOperator(
        task_id='end'
    )

    start >> get_top3_ram_loc >> write_top3_loc_to_gp >> end
