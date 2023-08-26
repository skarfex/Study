"""
Russian Central Bank Currency Exchange Rate to Greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET

from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-sarvarov-10',
    'poke_interval': 600
}


# TODO: create constants for url, xml and csv files
pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')

dag = DAG("vs_load_cbr",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-sarvarov-10']
    )


# TODO: not constant but {{ format_ds... }}
# TODO: delete vs_cbr.xml if exists


def is_weekday_func(execution_dt):
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    return exec_day in [0, 1, 2, 3, 4, 5]

weekday_only = ShortCircuitOperator(
    task_id='weekday_only',
    python_callable=is_weekday_func,
    op_kwargs={'execution_dt': '{{ ds }}'},
    dag=dag
    )

load_cbr_xml_script = '''
curl https://cbr.ru/scripts/xml_daily.asp?date_req=05/07/2022 | iconv -f Windows-1251 -t UTF-8 > /tmp/vs_cbr.xml
'''

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml', 
    bash_command=load_cbr_xml_script, 
    dag=dag
)


def export_xml_to_csv_func(): 
    parser = ET.XMLParser(encoding="UTF-8") 
    tree = ET.parse('/tmp/vs_cbr.xml', parser=parser)
    root = tree.getroot()
    
    with open('/tmp/vs_cbr.csv', 'w') as csv_file:
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
    # TODO: merge or clean the previous data batch
    pg_hook.copy_expert("COPY v_sarvarov_cbr FROM STDIN DELIMITER ','", '/tmp/vs_cbr.csv')


load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

weekday_only >> load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
