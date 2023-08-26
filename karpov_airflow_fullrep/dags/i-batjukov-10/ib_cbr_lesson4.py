"""
Igor Batyukov

Load CBR currencies to GreenPlum

"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 7, 1),
    'end_date': datetime(2022, 7, 15),
    'owner': 'i-batjukov-10',
    'poke_interval': 600
}

url_to_get = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req='
xml_path = '/tmp/ib_cbr.xml'
csv_path = '/tmp/ib_cbr.csv'

with DAG("ib_cbr_lesson4",
    schedule_interval='0 3 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ib_cbr_lesson4']
) as dag:

    def get_url_func(**kwargs):
        str_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d").date()
        return url_to_get + str_date.strftime('%d/%m/%Y')

    get_url = PythonOperator(
        task_id='get_url',
        python_callable=get_url_func,
        provide_context=True
    )

    bash_script = '''
    curl  {{ ti.xcom_pull(task_ids='get_url') }} | iconv -f Windows-1251 -t UTF-8 > /tmp/ib_cbr.xml
    '''

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=bash_script
    )

    def get_root(path_to_xml):
        import xml.etree.ElementTree as ET

        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(path_to_xml, parser=parser)
        root = tree.getroot()
        return root

    def export_xml_to_csv_func():
        import csv

        with open(csv_path, 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in get_root(xml_path).findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([get_root(xml_path).attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([get_root(xml_path).attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])

    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func,
    )

    eod = DummyOperator(
        task_id='eod'
    )

    def check_xml_date_func(**kwargs):
        xml_date = datetime.strptime(get_root(xml_path).attrib['Date'], '%d.%m.%Y').date()
        execution_date = datetime.strptime(kwargs['templates_dict']['execution_dt'], '%Y-%m-%d').date()
        if xml_date == execution_date:
            return ['export_xml_to_csv', 'load_csv_to_gp']
        else:
            logging.info('----------------------------------EOD---------------------------------------------')
            logging.info(f'Data for the current day({execution_date}) was not loaded.')
            logging.info('.xml file did not contain data for that day')
            logging.info('----------------------------------EOD---------------------------------------------')
            return 'eod'

    check_xml_date = BranchPythonOperator(
        task_id='check_xml_date',
        python_callable=check_xml_date_func,
        templates_dict={'execution_dt': '{{ ds }}'}
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        pg_hook.copy_expert("COPY i_batyukov_cbr FROM STDIN DELIMITER ','", csv_path)

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    get_url >> load_cbr_xml >> check_xml_date
    check_xml_date >> export_xml_to_csv >> load_csv_to_gp
    check_xml_date >> eod
