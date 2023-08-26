"""
Сложные пайплайны ч.2 - практика. Выгружаем данные из CBR.

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ang-semenova',
    'poke_interval': 600
}

with DAG("ang-semenova-cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['ang-semenova']
          ) as dag:


    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021 | iconv -f Windows-1251 -t UTF-8 > /tmp/lina_cbr.xml'
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/lina_cbr.xml', parser=parser)
        tree = ET.parse('/tmp/lina_cbr.xml', parser=parser)
        root = tree.getroot() # Корень нашего файла

        with open('/tmp/lina_cbr.csv', 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')]) # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                            [Name] + [Value.replace(',', '.')]) # Логируем все в log airflow, чтобы посмотреть  все ли хорошо

    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func,
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id ='conn_greenplum_write') # Создаем hook, записываем наш гринплан
        pg_hook.copy_expert("COPY ang_semenova_cbr FROM STDIN DELIMITER ','", '/tmp/lina_cbr.csv')

    load_to_gp = PythonOperator(
        task_id = 'load_to_gp',
        python_callable = load_csv_to_gp_func
    )

    export_cbr_xml >> xml_to_csv >> load_to_gp