"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-mosanu',
    'poke_interval': 600
}

# TODO: вынести url, файлы с xml и csv в константу

dag = DAG("andrei_load_cbr", # Меняем название нашего DAG
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mosanu']
          )

# TODO: Не константа, а {{ format_ds... }}
# TODO: удалять файл dina_cbr.xml, если он уже есть

load_cbr_xml_script = '''
curl https://www.cbr.ru/scripts/XML_daily.asp?date_req=08/06/2023 | iconv -f Windows-1251 -t UTF-8 > /tmp/and_cbr.xml
'''
load_cbr_xml = BashOperator(
    task_id='load_cbr_xml', # Меняем название в нашем task
    bash_command=load_cbr_xml_script, # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url
    dag=dag
)

def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8") # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
    tree = ET.parse('/tmp/and_cbr.xml', parser=parser)
    root = tree.getroot() # Корень нашего файла

    with open('/tmp/and_cbr.csv', 'w') as csv_file: # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
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

export_xml_to_csv = PythonOperator( # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)

def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id = 'conn_greenplum_write') # Создаем hook, записываем наш гринплан
    pg_hook.copy_expert("COPY andu_cbr FROM STDIN DELIMITER ','", '/tmp/and_cbr.csv')
    
load_csv_to_greenplum = PythonOperator(
    task_id = 'load_csv_to_GP',
    python_callable = load_csv_to_gp_func,
    dag= dag )

load_cbr_xml >> export_xml_to_csv >> load_csv_to_greenplum
# TODO: Мёрдж или очищение предыдущего батча
