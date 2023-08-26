"""
Доработка тестового дага

Задание:
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
Дни работы дага: с понедельника по субботу, но не по воскресеньям
Источник: greenplum.lab.karpov.courses таблица articles
Соединение: 'conn_greenplum' (Вариант решения — PythonOperator с PostgresHook)
Задача: забирать значение поля heading из строки с id, равным дню недели ds (понедельник = 1)
Вывод: в логах либо в XCom'е

"""
from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-astafeva-19',
    'poke_interval': 600
}

with DAG("s-astafeva-19-greenplum-articles",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-astafeva-19']
) as dag:

    # Функция получает на вход дату исполнения, берет из нее день недели
    # Возвращает True, если это не воскресенье
    def is_not_sunday_func(execution_dt):
        logging.info(execution_dt)
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day < 6

    # Оператор, который позволяет исполнять дальнейший шаг если is_not_sunday_func вернет True
    # Если is_not_sunday_func вернет False, дальнейшие шаги не исполняются
    is_not_sunday = ShortCircuitOperator(
        task_id='is_not_sunday',
        python_callable=is_not_sunday_func,
        op_kwargs={'execution_dt': '{{ tomorrow_ds }}'}
    )

    # Функция, которая возвращает значение поля heading, соответвующее дню недели даты исполнения
    def get_heading_from_greenplum(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        # Дни недели начинаются с ноля, а id c 1
        exec_day += 1
        # Создаем соединение с greenplum
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("get_heading_from_articles")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info('--------------')
        logging.info(f'{exec_day} of {execution_dt}')
        logging.info(query_res)
        logging.info('--------------')


    # Оператор. Вызывает функцию, которая подключается к greenplum и читает поле heading
    # из строки с id, соответствующему дню недели 
    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_from_greenplum,
        op_kwargs={'execution_dt': '{{ tomorrow_ds }}'}
    )

    # Собираем dag. Проверяем, что дата исполнения не воскресенье, читаем из greenplum
    is_not_sunday >> get_heading
