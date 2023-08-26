"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
from datetime import date
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-epifanov',
    'poke_interval': 600}

with DAG(dag_id="g-epifanov_l4_1_cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['g-epifanov', 'hw-2']
          ) as dag:

    # Load actual cbr data from url to xml
    today = date.today().strftime("%Y/%m/%d")
    url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=' + today

    load_cbr_xml_script = '''
    rm -f /tmp/g_epifanov_cbr.xml
    curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/g_epifanov_cbr.xml
    '''.format(url=url)

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script,
        dag=dag)

    # decode xml to csv table
    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/g_epifanov_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/g_epifanov_cbr.csv', 'w') as csv_file:
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
        python_callable=export_xml_to_csv_func,
        dag=dag)

    # load csv to greenplum

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER ','", '/tmp/g_epifanov_cbr.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag)

    # remove files
    remove_old_files = '''
    rm -f /tmp/g_epifanov_cbr.xml
    rm -f /tmp/g_epifanov_cbr.csv
    '''
    remove_files = BashOperator(
        task_id='remove_csv_and_xml',
        bash_command=remove_old_files,
        dag=dag)

    load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp >> remove_files
