"""
Загружаем данные из CBR и складываем в GreenPlum
"""
from multiprocessing import dummy
import os
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'p-ganzhela-11',
    'poke_interval': 600,
    'csv_filepath' : '/tmp/last_cbr.xml'
}

with DAG("p-ganzhela-11_load_cbr",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-ganzhela-11']
         ) as dag:


    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/dina_cbr.xml', parser=parser)
        root = tree.getroot()

        with open(DEFAULT_ARGS['csv_filepath'], 'w') as csv_file:
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

    # def load_csv_to_greenplum_func():
    #         pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    #         pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMITER ','", DEFAULT_ARGS['csv_filepath'])


    # dummy_task_outside = DummyOperator(task_id='date_outside_rande', dag=dag)

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='rm -f {filepath} > /dev/null && curl {url} | iconv -f Windows-1251 -t UTF-8 > {filepath}'.format(
            url="""https://www.cbr.ru/scripts/XML_daily.asp?date_req={{execution_date.format('dd/mm/yyyy')}}""",
            filepath = DEFAULT_ARGS['csv_filepath']
        ), dag=dag
    )

    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )


    # load_csv_to_greenplum = PythonOperator(
    #     task_id='load_csv_to_greenplum',
    #     python_callable=load_csv_to_greenplum_func
    # )

    # cond_date = BranchDateTimeOperator(
    #     task_id='datetime_branch',
    #     follow_task_ids_if_true=['export_cbr_xml','xml_to_csv'],
    #     follow_task_ids_if_false=['date_outside_range'],
    #     target_upper=datetime(2022, 3, 1),
    #     target_lower=datetime(2022, 3, 14),
    #     dag=dag,
    # )

    
    export_cbr_xml >> xml_to_csv
