"""
Exports data from cbr, converts to csv and loads to greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

import csv
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


URL = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021'
INPUT_FILE = '/tmp/cbr.xml'
OUTPUT_FILE = '/tmp/cbr.csv'
TABLE_NAME = 'dina_cbr'


def convert_xml2csv():
    if os.path.exists(INPUT_FILE):
        parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
        tree = ET.parse(INPUT_FILE, parser=parser)
        root = tree.getroot() # Корень нашего файла
        
        with open(OUTPUT_FILE, 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')]) # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')]) # Логируем все в log airflow, чтобы посмотреть  все ли хорошо
    else:
        print('ERROR: file not found')
        os.pwd()
        os.listdir()

def load_csv_to_gp_func(table_name):
        pg_hook = PostgresHook('conn_greenplum_write')
        # TODO: Мёрдж или очищение предыдущего батча
        pg_hook.copy_expert(f"COPY {table_name} FROM STDIN DELIMITER ','", OUTPUT_FILE)


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-kontsevaja-19',
    'poke_interval': 600,
    'queue': 'karpov_queue'
}

with DAG("export_from_cbr",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-kontsevaja-19']
) as dag:

    export_data = BashOperator(
        task_id='export_data',
        bash_command=f'curl {URL} | iconv -f Windows-1251 -t UTF-8 > {OUTPUT_FILE}',
        dag=dag
    )

    xml2csv = PythonOperator( # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
        task_id='xml2csv',
        python_callable=convert_xml2csv,
        dag=dag
    )

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        op_args=TABLE_NAME,
        dag=dag
    )


    export_data >> xml2csv >> load_csv_to_gp