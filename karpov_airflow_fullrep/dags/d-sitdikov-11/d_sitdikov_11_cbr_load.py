"""
Забираем данные из CBR и складывем в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd_sitdikov_11',
    'poke_interval': 600
}

with DAG("d_sitdikov_11_load_cbr",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d_sitdikov_11']
          ) as dag:

    load_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv - f Windows-1251 -t UTF-8 > /tmp/d_sitdikov_cbr.xml'.format(
            url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=04/12/2021'
        ),
        dag=dag
    )

    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/d_sitdikov_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/d_sitdikov_cbr.csv', 'w') as csv_file:
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
        dag=dag
    )

    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY d_sitdikov_11_cbr FROM STDIN DELIMETER ','", '/tmp/d_sitdikov_cbr.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp

