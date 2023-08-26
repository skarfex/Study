# == Урок 5. Задание ================================================
# 1. Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
# 2. С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
# 3. Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
# Примечание:
# * Можно использовать хук PostgresHook, можно оператор PostgresOperator
# * Предпочтительно использовать написанный вами оператор для вычисления top-3 локаций из API
# * Можно использовать XCom для передачи значений между тасками, можно сразу записывать нужное значение в таблицу
# * Не забудьте обработать повторный запуск каждого таска: предотвратите повторное создание таблицы, позаботьтесь об отсутствии в ней дублей

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator, get_current_context
from datetime import timedelta
from datetime import datetime
from a_sheremet_16_plugins.sam_ram_location_operator import SamRamLocationOperator

default_args = {
    'owner': 'a.sheremet-16',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 20),
    'schedule_interval': '0 14 * 1 *'
}

#Адрес и название временного файла
var_csv = '/tmp/top_locations_lst.csv'
var_table_name = 'public.a_sheremet_16_ram_location'

dag = DAG('sam_dag_hw_5'
          , catchup=False
          , default_args=default_args
          , tags=['sheremet_am'])

start = DummyOperator(task_id = 'start', dag=dag)
end = DummyOperator(task_id = 'end', dag=dag)

# Оператор для вычисления top-3 локаций из API
sam_get_location_top = SamRamLocationOperator(
    task_id = 'sam_get_location_top'
    , location_top_numbers = 3
    , to_csv = var_csv
    , dag=dag)

# Функция вставки итоговых данных данных в GP
def _copy_to_gp_func(input_path, table_name):
    pg_hook = PostgresHook('conn_greenplum_write')
    # очищение предыдущего батча
    q = f"TRUNCATE {table_name};"
    pg_hook.run(sql=q)
    print(f"Delete from {table_name} all rows is successful")
    # копирование нового батча через файл
    table_name_short = table_name.split(".",1)[1] #Оставляем только название таблицы (без схемы)
    pg_hook.copy_expert(f"COPY {table_name_short} FROM STDIN DELIMITER '^'", input_path)
    return print(f"Insert into {table_name} is successful\n Import file:\n {input_path}")

#Таск вставки итоговых данных данных в GP
sam_copy_to_gp = PythonOperator(
    task_id='sam_copy_to_gp'
    , python_callable=_copy_to_gp_func
    , op_kwargs={"input_path": var_csv, "table_name": var_table_name}
    , dag=dag)

start >> sam_get_location_top >> sam_copy_to_gp >> end