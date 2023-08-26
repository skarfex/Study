"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner': 'a-martyshkin-15',
    'poke_interval': 600
}

# TODO: вынести url, файлы с xml и csv в константу


dag = DAG("martyshkin_load_cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-martyshkin-15']
          )


def get_yesterday_date():
    yesterday = date.today() - timedelta(1)
    return yesterday.strftime('%d/%m/%Y')


# TODO: Не константа, а {{ format_ds... }}
# TODO: удалять файл dina_cbr.xml, если он уже есть


load_cbr_xml_script = F'curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={get_yesterday_date()} | iconv -f Windows-1251 -t UTF-8 > /tmp/martyshkin_cbr.xml'

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=load_cbr_xml_script,
    dag=dag
)


def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse('/tmp/martyshkin_cbr.xml', parser=parser)
    root = tree.getroot()

    with open('/tmp/martyshkin_cbr.csv', 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        logging.info(f'**** ДАТА ДЛЯ ПАРСИНГА: {get_yesterday_date()} ****')
        logging.info(f'**** URL: https://www.cbr.ru/scripts/XML_daily.asp?date_req={get_yesterday_date()} ****')
        logging.info(f'>>> Парсим CRB')
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
        logging.info(f'>>> Готово')

export_xml_to_csv = PythonOperator(
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

def delete_to_gp():
    DELETE = f'DELETE FROM a_martyshkin_15_cbr_data'
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    pg_hook.run(DELETE)


delete_to_gp = PythonOperator(
    task_id='delete_to_gp',
    python_callable=delete_to_gp,
    dag=dag
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    print('************* Загрузка ******************')
    pg_hook.copy_expert("COPY a_martyshkin_15_cbr_data FROM STDIN DELIMITER ','", '/tmp/martyshkin_cbr.csv')
    print('************* Готово ******************')

load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

load_cbr_xml >> export_xml_to_csv >> delete_to_gp >> load_csv_to_gp
