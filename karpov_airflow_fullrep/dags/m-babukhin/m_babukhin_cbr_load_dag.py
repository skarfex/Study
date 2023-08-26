"""
Складываем курс валют в PosgreSQL
"""

import logging
import csv
import xml.etree.ElementTree as ET
# from datetime import date

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-babukhin',
    'poke_interval': 600
}

# TODO: вынести url, файлы с xml и csv в константу

dag = DAG("babukhin_load_cbr",  # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['m-babukhin']
          )

# TODO: Не константа, а {{ format_ds... }}
# TODO: удалять файл dina_cbr.xml, если он уже есть

# current_date = date.today().strftime("%d/%m/%Y")

# cbr_url = f'https://cbr.ru/scripts/xml_daily.asp?date_req={current_date}'

load_cbr_xml_script = f'curl https://cbr.ru/scripts/xml_daily.asp?date_req=14/02/2023 | iconv -f Windows-1251 -t UTF-8 > /tmp/cbr_data.xml'

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',  # Меняем название в нашем task
    # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url
    bash_command=load_cbr_xml_script,
    dag=dag
)


def export_xml_to_csv_func():
    # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse('/tmp/cbr_data.xml', parser=parser)
    root = tree.getroot()

    with open('/tmp/cbr_data.csv', 'w', encoding='UTF-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for Valute in root.findall('Valute'):
            NumCode = Valute.find('NumCode').text
            CharCode = Valute.find('CharCode').text
            Nominal = Valute.find('Nominal').text
            Name = Valute.find('Name').text
            Value = Valute.find('Value').text
            writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']]
                            + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')])
            """
            Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку,для того,
            чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит,
            что это переход на новое значение.
            """
            logging.info([root.attrib['Date']] + [Valute.attrib['ID']]
                         + [NumCode] + [CharCode] + [Nominal] +
                         [Name] + [Value.replace(',', '.')])
            # Логируем все в log airflow, чтобы посмотреть  все ли хорошо


export_xml_to_csv = PythonOperator(  # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)


def load_csv_to_pg_func():  # Создаем hook, записываем наш csv в PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_db_conn')
    pg_hook.copy_expert(
        "COPY cbr_data FROM STDIN DELIMITER ','", '/tmp/cbr_data.csv')


load_csv_to_postgres = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_pg_func,
    dag=dag
)

# TODO: Мёрдж или очищение предыдущего батча


load_cbr_xml >> export_xml_to_csv >> load_csv_to_postgres
