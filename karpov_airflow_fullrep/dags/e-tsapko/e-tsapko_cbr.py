"""
Забираем данные из ЦБР и складываем в Greenplum
"""
import csv
import logging
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-tsapko',
    'poke_interval': 120
}

dag = DAG("e-tsapko_load_cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-tsapko']
          )

load_cbr_xml_script = '''
    curl https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2022 | iconv -f Windows-1251 -t UTF-8 > /tmp/e_tsapko_cbr_2.xml
    '''

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=load_cbr_xml_script,
    dag=dag
)

def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse('/tmp/e_tsapko_cbr_2.xml', parser=parser)
    root = tree.getroot()

    with open('/tmp/e_tsapko_cbr_2.csv', 'w') as csv_file:
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

def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    pg_hook.copy_expert("COPY public.e_tsapko_cbr FROM STDIN DELIMITER ','", '/tmp/e_tsapko_cbr_2.csv')

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
