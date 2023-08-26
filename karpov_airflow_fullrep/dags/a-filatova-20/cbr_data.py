"""
Складываем курс валют в GreenPlum (Меняем описание нашего дага)
"""
from datetime import datetime

from airflow import DAG
import logging
import csv
import xml.etree.ElementTree as ET  # Импортировали из библиотеки xml элемент tree и назвали его ET

from airflow.hooks.postgres_hook import PostgresHook  # c помощью этого hook будем входить в наш Greenplan
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2023, 5, 5),
    'end_date': datetime(2023, 5, 12),
    'owner': 'a-filatova-20',
    'poke_interval': 600
}

url = 'https://www.cbr.ru/scripts/XML_daily.asp?date_req='
xml_path = '/tmp/data_cbr.xml'
csv_path = '/tmp/data_cbr.csv'

## https://crontab.guru/#0_1_*_*_1-5
## если бы я хотела задать расписание в 01:00 каждые пн-пт
dag = DAG("fil_load_cbr",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-filatova-20']
          )


def get_url_func(**kwargs):
    str_date = datetime.strptime(kwargs['ds'], "%Y-%m-%d").date()
    return url + str_date.strftime('%d/%m/%Y')


get_url = PythonOperator(
    task_id='get_url',
    python_callable=get_url_func,
    provide_context=True,
    dag=dag
)


# def xml_path_ret(**kwargs):
#     return xml_path
#
#
# get_xml = PythonOperator(
#     task_id='get_xml',
#     python_callable=xml_path_ret,
#     provide_context=True,
#     dag=dag
# )

bash_script = '''
curl  {{ ti.xcom_pull(task_ids='get_url') }} | iconv -f Windows-1251 -t UTF-8 > /tmp/data_cbr.xml
'''
# bash_script = f'''
# curl  {{{{ ti.xcom_pull(task_ids='get_url') }}}} | iconv -f Windows-1251 -t UTF-8 > {xml_path}
# '''

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',  # Меняем название в нашем task
    bash_command=bash_script,
    # Вставляем нашу команду, так как она не помещается в единую строку, то используем форматер url
    dag=dag
)


def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")  # Создаем Parser формата UTF-8, натравливаем его на нужный файл
    tree = ET.parse(xml_path, parser=parser)
    root = tree.getroot()  # Корень нашего файла

    with open(csv_path, 'w') as csv_file:  # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
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
                         [Name] + [
                             Value.replace(',', '.')])  # Логируем все в log airflow, чтобы посмотреть  все ли хорошо


export_xml_to_csv = PythonOperator(  # Xml перекладываем в csv, так как с csv все базы работают гораздо лучше
    task_id='export_xml_to_csv',
    python_callable=export_xml_to_csv_func,
    dag=dag
)


def load_csv_to_gp_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    pg_hook.copy_expert("COPY a_filatova_20_cbr FROM STDIN DELIMITER ','", csv_path)


load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

get_url >> load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
