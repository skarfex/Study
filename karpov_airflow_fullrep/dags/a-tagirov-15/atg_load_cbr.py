"""
Забираем данные из ЦБ и складываем курс валют в GreenPlum, многое еще предстоит сделать
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
    'owner': 'a-tagirov-15',
    'poke_interval':600
}


with DAG('atg_load_cbr', # так наш даг будет называться в Airflow
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=["atg"]
         ) as dag:

    load_cbr_xml_script = '''
    curl https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021 | iconv -f Windows-1251 -t UTF-8 > /tmp/atg_cbr.xml
    '''

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script,
        dag=dag
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")  # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
        tree = ET.parse('/tmp/atg_cbr.xml', parser=parser)
        root = tree.getroot()  # Корень нашего файла

        with open('/tmp/atg_cbr.csv',
                  'w') as csv_file:  # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',',
                                                        '.')])  # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',',
                                                     '.')])  # Логируем все в log airflow, чтобы посмотреть  все ли хорошо


    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )

    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY atg_cbr FROM STDIN DELIMITER ','", '/tmp/atg_cbr.csv')

    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func,
    )

    load_cbr_xml >> xml_to_csv >> load_csv_to_greenplum
