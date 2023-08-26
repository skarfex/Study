"""
Учебный даг на декораторах
Работает с 1 марта 2022 по 14 марта 2022
Записыавет в логи сроку из бд, номер которой соответсует дню недели даты исполнения дага
"""
from datetime import datetime
from logging import info

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    'owner': 'v-talnikov-12',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
}


@dag(
    dag_id="v-talnikov-12_hard_decor_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v', 'v-talnikov-12']
)
def main_dag():

    echo_ds = BashOperator(task_id='echo_ds', bash_command='echo {{ ds }}')

    @task()
    def notification_func(exec_date):
        print('Время пришло...' + exec_date)
        info("The time has come...")

    @task()
    def res_from_greenplum(exec_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        id = datetime.strptime(exec_date, '%Y-%m-%d').weekday() + 1  # преобразование дня недели из даты
        # print('Пришло exec_date' + str(id))
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(id))  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        info(str(one_string))

    # notification_func(echo_ds.output)
    res_from_greenplum(echo_ds.output)


dag = main_dag()
