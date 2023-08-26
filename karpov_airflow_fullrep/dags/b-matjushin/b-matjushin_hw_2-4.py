"""
Курс "Инженер данных". Модуль 2. Задание к уроку #4.

Нужно доработать даг, который вы создали на прошлом занятии. Он должен:
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания
или операторов ветвления);
Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри;
Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS;
Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели ds (понедельник=1, вторник=2, ...);
Выводить результат работы в любом виде: в логах либо в XCom'е;
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
import pendulum
import logging
import csv
import xml.etree.ElementTree as ET

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='Europe/Moscow'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='Europe/Moscow'),
    'catchup': True,
    'owner': 'b-matjushin',
    'poke_interval': 600
}

cbr_url = 'https://cbr.ru/scripts/xml_daily.asp?date_req='
xml_file = '/tmp/b-matjushin_crb.xml'
csv_file = '/tmp/b-matjushin_crb.csv'

with DAG(
    'b-matjushin_hw_2-4',
    schedule_interval='0 23 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["b-matjushin"]
) as dag:

    date_url = '{{ macros.ds_format(ds, "%Y-%m-%d", "%d/%m/%Y") }}'
    load_cbr_xml_script = f'curl {cbr_url}{date_url}| iconv -f Windows-1251 -t UTF-8 > {xml_file}'

    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_script,
    )

    def export_xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(xml_file, parser=parser)
        root = tree.getroot()

        with open(csv_file, 'w') as file_csv:
            writer = csv.writer(file_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
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


    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func
    )

    clean_date = PostgresOperator(
        task_id="clean_date",
        postgres_conn_id='conn_greenplum_write',
        sql="DELETE FROM b_matjushin_currency WHERE date = '{{ ds }}'"
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum')
        pg_hook.copy_expert("COPY b_matjushin_currency FROM STDIN DELIMITER ','", csv_file)

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    def article_task_func(**kwargs):
        ti = kwargs['ti']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        article_num = ti.start_date.weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_num}')
        one_string = cursor.fetchone()[0]
        ti.xcom_push(value=one_string, key='article_heading')


    article_task = PythonOperator(
        task_id='article_task',
        python_callable=article_task_func
    )

    load_cbr_xml >> export_xml_to_csv >> clean_date >> load_csv_to_gp >> article_task