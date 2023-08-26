"""
    Задание 5
"""

from airflow import DAG
import datetime as dt
import requests

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException

DEFAULT_ARGS = {
    'owner': 'p-abramov-17'
    , 'poke_interval': 300
}

with DAG(
        dag_id="p_abramov_17_lesson_5"
        , schedule_interval='@daily'
        , default_args=DEFAULT_ARGS
        , max_active_runs=1
        , start_date=dt.datetime(2023, 2, 11)
        , tags=['p-abramov-17']
) as dag:
    # Task 0 - Start
    dummy = DummyOperator(task_id="dummy")


    # откидываем сообщение в лог
    def fn_print_log(str_):
        print('-----------------')
        print(str_)  # логируем
        print('-----------------')


    # Создаем таблицу, если ранее она не была создана
    create_table_if_not_exists_str_ = '''
        CREATE table if not exists public.p_abramov_17_ram_location 
        (
            id int4
            , name varchar
            , type varchar
            , dimension varchar
            , resident_cnt int4 
        )
        DISTRIBUTED BY (id);
    '''


    # Тask 1 - создаем таблицу, если ее еще не было
    def py_fn_create_table_if_not_exists():
        fn_print_log('пересоздаем таблицу')  # логируем

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(create_table_if_not_exists_str_)
        conn.commit()
        conn.close()


    py_create_table_if_not_exists = PythonOperator(
        task_id='py_create_table_if_not_exists'
        , python_callable=py_fn_create_table_if_not_exists
    )

    # адрес
    url_str_ = 'https://rickandmortyapi.com/api/location?page={page}'


    # Task 2 - кол-во страниц
    def py_fn_get_cnt_page(**kwargs):
        fn_print_log('запрашиваем кол-во страниц')  # логируем
        try:
            result_ = requests.get(url_str_.format(page='1'))
            if result_.status_code == 200:
                fn_print_log('Успешно запросили страницы')  # логируем
                page_cnt = result_.json().get('info').get('pages')
                fn_print_log(f'кол-во страниц = {page_cnt}')
                kwargs['ti'].xcom_push(value=page_cnt, key='page_cnt')
            else:
                fn_print_log("HTTP STATUS {}".format(result_.status_code))
                raise AirflowException('Error in load page count')
        except:
            fn_print_log('Ошибка обращения по АПИ')  # логируем


    py_get_cnt_page = PythonOperator(
        task_id='py_get_cnt_page'
        , python_callable=py_fn_get_cnt_page
    )

    # Task 3 - собираем инфу по всем страницам в словарь
    def py_fn_get_info_by_page(**kwargs):
        fn_print_log('собираем информацию по страницам')  # логируем
        page_cnt = kwargs['ti'].xcom_pull(task_ids='py_get_cnt_page', key='page_cnt')
        list_all_location_ = []
        for i in range(page_cnt):
            fn_print_log(f'читаем страницу = {i+1}')
            data_ = requests.get(url_str_.format(page=str(i + 1))).json()
            list_ = data_['results']
            for loaction_ in list_:
                id_ = loaction_['id']
                name_ = loaction_['name']
                type_ = loaction_['type']
                dimension_ = loaction_['dimension']
                resident_cnt_ = len(loaction_['residents'])
                list_all_location_.append((id_, name_, type_, dimension_, resident_cnt_))

        fn_print_log(list_all_location_)  # логируем
        list_top_3 = sorted(list_all_location_, key=lambda x: x[4], reverse=True)[:3]
        fn_print_log(list_top_3)  # логируем
        kwargs['ti'].xcom_push(value=list_top_3, key='list_top_3')


    py_get_info_by_page = PythonOperator(
        task_id='py_get_info_by_page'
        , python_callable=py_fn_get_info_by_page
    )

    # Task 4 - обновляем информацию в базе
    # Так как ситуации могут быть разные, то считаю, что если жошли до этого шага - значит у нас максимальнго достоверная
    # и актуальная информация. Значит полность чистим таблицу и заливаем свежие данные
    def py_fn_insert_result(**kwargs):
        fn_print_log('Очищаем таблицу')  # логируем
        truncate_str = 'truncate table public.p_abramov_17_ram_location;'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(truncate_str)

        list_top_3 = kwargs['ti'].xcom_pull(task_ids='py_get_info_by_page', key='list_top_3')

        if len(list_top_3) > 0:
            fn_print_log('вставляем данные')  # логируем
            for row in list_top_3:
                fn_print_log(row)  # логируем
                query_insert = 'insert into public.p_abramov_17_ram_location values {}'.format(row)
                cursor.execute(query_insert)
        else:
            fn_print_log('данных нет')  # логируем

        conn.commit()
        conn.close()


    py_insert_result = PythonOperator(
        task_id='py_insert_result'
        , python_callable=py_fn_insert_result
    )

    dummy >> py_create_table_if_not_exists >> py_get_cnt_page >> py_get_info_by_page >> py_insert_result
