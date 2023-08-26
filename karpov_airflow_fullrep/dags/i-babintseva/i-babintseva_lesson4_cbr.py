"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging
import csv
import xml.etree.ElementTree as ET
import os

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2023, 6, 5),
    'end_date': datetime(2023, 6, 9),
    'owner': 'i-babintseva',
    'poke_interval': 600
}

url = 'https://www.cbr.ru/scripts/XML_daily.asp'
xml_filename = '/tmp/i_babintseva_cbr.xml'
csv_filename = '/tmp/i_babintseva_cbr.csv'

dag = DAG("i-babintseva_lesson4_cbr",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['i-babintseva']
          )

load_cbr_xml_script = f'rm -f {xml_filename};' \
                      f'curl {url}?date_req='+"{{ macros.ds_format(ds, '%Y-%m-%d', '%d/%m/%Y') }} "+ f'| iconv -f Windows-1251 -t UTF-8 > {xml_filename}'

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=load_cbr_xml_script,
    dag=dag
)


def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(xml_filename, parser=parser)
    root = tree.getroot()

    with open(csv_filename, 'w') as csv_file:
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
            logging.info(f"File path: {os.path.abspath(csv_filename)}")

export_xml_to_csv = PythonOperator(
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    pg_hook.run(f'truncate table i_babintseva_cbr')
    pg_hook.copy_expert("COPY i_babintseva_cbr FROM STDIN DELIMITER ','", csv_filename)

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
