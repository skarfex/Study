"""
Загрузка курса валют
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import csv
import xml.etree.ElementTree as ET # Импортировали из библиотеки xml элемент tree и назвали его ET

DEFAULT_ARGS = {
    'start_date': days_ago(2), # дата начала генерации DAG Run-ов
    'owner': 'n-novikova-16',  # владелец
    'poke_interval': 600       # задает интервал перезапуска сенсоров (каждые 600 с.)
}

with DAG("n-novikova-lesson-4-curs", # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='@daily', # расписание
    default_args=DEFAULT_ARGS,  # дефолтные переменные
    max_active_runs=1,          # позволяет держать активным только один DAG Run
    tags=['n-novikova-16']      # тэги
) as dag:

    export_data = BashOperator(
        task_id='export_data', # уникальный идентификатор таски внутри DAG
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/nvn_curs.xml'.format(
            url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021'
        ),
        #https: // rickandmortyapi.com / documentation /  # location
        dag=dag
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")  # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/nvn_curs.xml', parser=parser)
        tree = ET.parse('/tmp/nvn_curs.xml', parser=parser)
        root = tree.getroot()  # Корень нашего файла

        with open('/tmp/nvn_curs.csv', 'w') as csv_file:  # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])  # Из атрибута root берем дату, из атрибута valute берем id, в конце заменяем запятую на точку, для того, чтобы при сохранении в формате csv, если оставить запятую в нашем поле, формат решит, что это переход на новое значение
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])  # Логируем все в log airflow, чтобы посмотреть  все ли хорошо

    xml_to_csv = PythonOperator(
        task_id='hello_world',
        python_callable=xml_to_csv_func, # ссылка на функцию, выполняемую в рамках таски
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # Создаем hook, записываем наш гринплан
        pg_hook.copy_expert("COPY public.n_novikova_16_cbr from STDIN delimiter ','",'/tmp/nvn_curs.csv')

    export_xml_to_csv = PythonOperator(  # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
        task_id='export_xml_to_csv',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )


    export_data >> xml_to_csv >> export_xml_to_csv