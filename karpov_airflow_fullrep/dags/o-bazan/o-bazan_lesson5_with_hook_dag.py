'''
Даг, посвященный заданию 5-го урока из блока "ETL"
Даг находит ТОП-3 локаций по количеству резидентов в API Rick&Morty с помощью кастомного оператора TOPLocationOperator
и загружает данные в таблицу o_bazan_ram_location в GreenPlum

Реализация c кастомным хуком

Примечание: идемподентность реализована полным очищением таблицы перед загрузкой в нее новых данных,
тк если в источнике изменится информация о локациях, и ТОП-3 станет новым, то таблица будет содержать неверную информацию
'''

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from o_bazan_plugins.o_bazan_ram_operator import TOPLocationOperatorWIthHook

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': days_ago(1)
}

dag = DAG(
dag_id = "o-bazan_lesson5_with_hook_dag",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    tags=['lesson5', 'Rick and Morty', 'o-bazan']
)

top_location_table = 'o_bazan_ram_location' # название таблицы в GreenPlum, с которой будем работать


# Получение из API ТОП-3 локации по количеству резидентов
top3_location_task = TOPLocationOperatorWIthHook(
    task_id='top3_location_task',
    #top_num=3,
    dag=dag
)

# Подготовка таблицы в GreenPlum
def prepare_table_func():
    # Подключение к БД
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

    # Проверка на cуществование таблицы
    is_exists_query = f'''
                        SELECT COUNT(*)
                        FROM pg_catalog.pg_tables
                        WHERE schemaname='public' AND tablename='{top_location_table}';'''
    is_exists = pg_hook.get_first(is_exists_query)[0]

    # Если таблица уже есть, то очищаем ее
    if is_exists:
        pg_hook.run(f'TRUNCATE TABLE {top_location_table};')

    # Если таблицы нет, то создаем ее
    else:
        create_table_query = f'''
                        CREATE TABLE {top_location_table}(
                             id INT PRIMARY KEY,
                             name VARCHAR(50),
                             type VARCHAR(50),
                             dimension VARCHAR(100),
                             resident_cnt INT
                             );
                        '''
        pg_hook.run(create_table_query)

prepare_table_task = PythonOperator(
    task_id='prepare_table_task',
    python_callable=prepare_table_func,
    dag=dag
)

# Загрузка данных о ТОП-3 локациях в GreenPlum
def load_data_in_greenplum_func(**kwargs):
    # Забираем данные о ТОП-3 локациях из xcom в location_data
    location_data = kwargs['ti'].xcom_pull(task_ids='top3_location_task', key='return_value')

    # Подключение к БД
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

    # Загрузка данных в таблицу
    load_data_query = '''
        INSERT INTO {}
        VALUES {}
        '''
    for location in location_data:
        pg_hook.run(load_data_query.format(top_location_table, location))

load_data_in_greenplum_task = PythonOperator(
    task_id='load_data_in_greenplum_task',
    python_callable=load_data_in_greenplum_func,
    dag=dag
)

top3_location_task >> prepare_table_task >> load_data_in_greenplum_task