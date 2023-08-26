"""
Урок 4
СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 2.
ПРАКТИКА: РАБОТА С ДАННЫМИ
Задание
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'owner': 'e-gulymedden-21',
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("e-gulymedden-21-les-4-load-xml",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-gulymedden-21']
         ) as dag:

    load_cbr_xml_script = '''
    curl {url} | iconv -f Windows-1251 -t UTF-8 > {xml_path}
    '''.format(url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021',
               xml_path='/tmp/yerkebulan_cbr.xml')

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script
    )

    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding='UTF-8')
        tree = ET.parse('/tmp/yerkebulan_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/yerkebulan_cbr.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])


    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY yerkebulan_cbr FROM STDIN DELIMITER ','", '/tmp/yerkebulan_cbr.csv')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
