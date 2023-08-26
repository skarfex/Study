"""
Забираем данные из cbr.ru и складываем в GreenPlum
"""
from typing import Tuple, Any

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
    'owner': 'Zhbanova',
    'poke_interval': 600
}

with DAG("zhbanova_second",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-zhbanova']
          ) as dag:

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/e_zhbanova_cbr.xml'.format(
            url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=22/06/2023'
        )
    )

    def xml_to_csv_func():
        # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/cbr.xml', parser=parser)
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/e_zhbanova_cbr.xml', parser=parser)
        # Корень нашего файла
        root = tree.getroot()

        # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
        with open('/tmp/e_zhbanova_cbr.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того,
                # чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход
                # на новое значение
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                # Логируем все в log airflow, чтобы посмотреть  все ли хорошо
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])

    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )

    def load_csv_to_greenplum_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY e_zhbanova_cbr FROM STDIN DELIMITER ','", "/tmp/e_zhbanova_cbr.csv")

    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func
    )

    export_cbr_xml >> xml_to_csv >> load_csv_to_greenplum

    dag.doc_md = __doc__