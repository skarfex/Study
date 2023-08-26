
from datetime import datetime as dt
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
DEFAULT_ARGS = {
    'start_date': dt(2022, 3, 1),
    'end_date': dt(2022, 3, 14),
    'owner': 'g-zatulo',
    'poke_interval': 600
}

@dag(
    dag_id="g_zatulo_l_4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=2,
    tags=['g-zatulo']
)
def g_zatulo_dag_le4():
    @task
    def connectGP_func(**kwargs):
        logging.getLogger().setLevel(logging.INFO)
        exec_d = kwargs['execution_date']
        logging.info(str(exec_d), "- execution date")
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        logging.info("hook initialized")
        conn = pg_hook.get_conn()  # берём из него соединение
        logging.info("got connection")
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        logging.info("got cursor")
        weekday = str(exec_d.isoweekday())
        logging.info(weekday)
        sql_req = 'SELECT heading FROM articles WHERE id ='+weekday
        logging.info(sql_req)
        cursor.execute(sql_req)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  #  если вернулось единственное значение
        logging.info(query_res)

    connectGP_func()


g_zatulo_l_4 = g_zatulo_dag_le4()


