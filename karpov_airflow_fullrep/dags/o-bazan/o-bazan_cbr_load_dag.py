"""
Даг, загружающий каждый день в 02:00 с официального сайта ЦБ курсы валют за текущий день в таблицу GreenPlum o_bazan_cbr
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import timedelta

from airflow.hooks.postgres_hook import PostgresHook # на базовом уровне можно использовать для GreenPlum, тк под капотом - Postgres
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'o-bazan',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG("o-bazan_cbr_load_dag",
          schedule_interval='0 2 * * *',
          default_args=DEFAULT_ARGS,
          tags=['lesson4', 'o-bazan']
          )

xml_file_path = '/tmp/ob_cbr.xml' # путь к файлу, временно хранящему курсы валют в формате xml
csv_file_path = '/tmp/ob_cbr.csv' # путь к файлу, временно хранящему курсы валют в формате csv
table_name = 'o_bazan_cbr' # имя целевой таблицы в GreenPlum

# Формирование bash команды для забора данных по курсу ЦБ за день исполнения дага и загрузки их в xml файл
def get_bash_command_func(**kwargs):
    # Форматирование даты исполнения дага дд/мм/гггг
    formatted_load_date = datetime.strptime(str(kwargs['ds']), "%Y-%m-%d").strftime("%d/%m/%Y/")
    logging.info('________________________________________________________________')
    logging.info(f'Дата исполнения дага: {formatted_load_date}')
    logging.info('________________________________________________________________')
    # Формирование url
    url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={formatted_load_date}'
    # Формирование bash команды
    bash_command = f"curl {url} | iconv -f Windows-1251 -t UTF-8 > {xml_file_path}"
    # Явно передаем bash команду в xcom
    kwargs['ti'].xcom_push(value=bash_command, key='bash_command')


get_bash_command = PythonOperator(
    task_id='get_bash_command',
    python_callable=get_bash_command_func,
    provide_context=True,
    dag=dag
)

# Исполнение bash команды
load_to_xml = BashOperator(
    task_id='load_to_xml',
    bash_command="{{ ti.xcom_pull(task_ids='get_bash_command', key='bash_command') }}",
    dag=dag
)

# Проверка на нерабочий день
def holiday_check_func(**kwargs):
    # Дата исполнения дага в формате дд.мм.гггг
    date_real = datetime.strptime(str(kwargs['ds']), "%Y-%m-%d").strftime("%d.%m.%Y")

    with open(xml_file_path, 'r') as file:
        content = file.read()
        logging.info('________________________________________________________________')
        logging.info('Содержимое временного xml файла:')
        logging.info(content)
        logging.info('________________________________________________________________')
        # Дата обновления данных по курсу валют
        date_xml = content.split('ValCurs Date="')[1].split('"')[0]

    # Если date_xml не совпадает с date_real, то данных по дате исполнения дага в API нет, значит день нерабочий
    if date_xml == date_real:
        return True
    else:
        return False
        logging.info('________________________________________________________________')
        logging.info('Данные по курсу валют за сегодняшний день отсутствуют')
        logging.info('________________________________________________________________')

holiday_check = ShortCircuitOperator(
    task_id='holiday_check',
    python_callable=holiday_check_func,
    provide_context=True,
    dag=dag
)

# Преобразование данных из xml в csv
def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8
    tree = ET.parse(xml_file_path, parser=parser) # Преобразуем xml файл к древовидной структуре
    root = tree.getroot() # Корень xml файла

    # Открываем csv для записи, в который будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
    with open(csv_file_path, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL) # Возвращает объект, ответственный за преобразование данных в строки с заданным разделителем

        # Для каждой валюты записываем данные в соответствующую переменную
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text

            # Запись в csv
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')])

            logging.info('________________________________________________________________')
            logging.info('Содержимое временного csv файла:')
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                         [Name] + [Value.replace(',', '.')])
            logging.info('________________________________________________________________')


export_xml_to_csv = PythonOperator(
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

# Проверка на сущестование соответствуещей таблицы (IF EXISTS нет в текущей версии) и ее создание в случае отсутствия
def create_table_if_not_exists_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    is_exists = pg_hook.get_first(f"SELECT COUNT(*) FROM pg_class WHERE relname='{table_name}';")[0]
    if not is_exists:
        pg_hook.run(f'''CREATE TABLE {table_name} (
                       dt TEXT,
                       id TEXT,
                       num_code TEXT,
                       char_code TEXT,
                       nominal TEXT,
                       nm TEXT,
                       value TEXT
                       );''')

create_table_if_not_exists = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_table_if_not_exists_func,
    dag=dag
)

# Загрузка данных из csv в таблицу GreenPlum
def load_csv_to_gp_func():
    # Подключение к БД
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')

    # Создание временной таблицы (в связи с настройками GreenPlum, не имею права на создание temporary table)
    pg_hook.run(f'''
            CREATE TABLE temp_{table_name} (
               dt TEXT,
               id TEXT,
               num_code TEXT,
               char_code TEXT,
               nominal TEXT,
               nm TEXT,
               value TEXT
            );''', autocommit=True)

    # Копирование данных из CSV файла во временную таблицу
    pg_hook.copy_expert(f"COPY temp_{table_name} FROM STDIN DELIMITER ','", csv_file_path)
    pg_hook.get_conn().commit()

    # Обновление данных в основной таблице (добавляем только те строки, которых еще нет в основной таблице)
    pg_hook.run(f'''
            INSERT INTO {table_name}
            SELECT temp.dt, temp.id, temp.num_code, temp.char_code, temp.nominal, temp.nm, temp.value
            FROM temp_{table_name} temp
            LEFT JOIN {table_name} main ON temp.dt = main.dt AND temp.id = main.id
            WHERE main.dt IS NULL;
        ''', autocommit=True)

    # Удаление временной таблицы
    pg_hook.run(f'DROP TABLE temp_{table_name}', autocommit=True)

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

# Удаление временных csv и xml файлов
file_cleaner = BashOperator(
    task_id='file_cleaner',
    bash_command=f'rm {xml_file_path} {csv_file_path}',
    dag=dag
)

get_bash_command >> load_to_xml >> holiday_check >> export_xml_to_csv >> create_table_if_not_exists >> load_csv_to_gp >> file_cleaner