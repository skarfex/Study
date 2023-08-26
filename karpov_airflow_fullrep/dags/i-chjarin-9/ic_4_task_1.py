"""
Курс Инженер данных
Модуль 4 Автоматизация ETL-процессов
Урок 4 Сложные пайплайны, часть 2
Задание
Задание
Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:

Работать с понедельника по субботу, но не по воскресеньям
(можно реализовать с помощью расписания или операторов ветвления)

Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте
его самостоятельно в вашем личном Airflow. Параметры соединения:

Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года


"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'i-chjarin-9',
    'poke_interval': 60
}

with DAG("ic_4_task_1",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-chjarin-9']
) as dag:
    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")  # Создаем Parser формата UTF-8, натравливаем его на нужный файл ('/tmp/dina_cbr.xml', parser=parser)
        tree = ET.parse('/tmp/ic_cbr.xml', parser=parser)
        root = tree.getroot()  # Корень нашего файла

        with open('/tmp/ic_cbr.csv', 'w') as csv_file:  # Открываем csv в которую будем писать построчно каждый элемент, который нас интересует: Valute, NumCode и т.д.
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

    def load_csv_to_greenplum_func():
        pg_hook=PostgresHook(postgres_conn_id='conn_greenplum')
        pg_hook.copy_expert("COPY i_chjarin_9_cbr FROM STDIN DELIMITER ',' ", '/tmp/ic_cbr.csv')

    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/ic_cbr.xml'.format(
            url='https://cbr.ru/scripts/xml_daily.asp?date_req=25/01/2023'
        ),
        dag=dag
    )

    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=export_xml_to_csv_func
    )

    load_csv_to_greenplum = PythonOperator(
        task_id='load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum_func,
        dag=dag
    )

    export_cbr_xml >> xml_to_csv >> load_csv_to_greenplum