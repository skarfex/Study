"""
This is a test DAG for HomeWork!!
We import currencies of CBR1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv

from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'schedule_interval': "0 0 * * 1-5",
    'owner': 'm-scherbinin-2',
    'poke_interval': 600
}

with DAG('marsel_dag',
         schedule_interval='@daily',
         default_args=default_args,
         max_active_runs=1,
         tags=['m-scherbinin']) as dag1:
    echo_ds1 = BashOperator(
        task_id='echo_ds1',
        bash_command='echo {{ ds }}',
        dag=dag1
    )


    # probe_dummy = DummyOperator(
    #     owner='m-scherbinin-2',
    #     task_id='probe_dummy',
    #     trigger_rule='one_success'
    # )
    # url_export_xml = '''
    # curl https://www.cbr.ru/scripts/XML_daily.asp?date_req=10/06/2022 | iconv -f Windows-1251 -t UTF-8 > Desktop/IT/KARPOV_Courses/mars_cbr.xml
    # '''
    #
    # export_cbr_xml = BashOperator(
    #     task_id='export_cbr_xml',
    #     bash_command=url_export_xml,
    #     dag=dag1
    # )
    #
    # def xml_to_csv_func():
    #     parser = ET.XMLParser(encoding='UTF-8')
    #     tree = ET.parse('Desktop/IT/KARPOV_Courses/mars_cbr.xml', parser=parser)
    #     root = tree.getroot()
    #
    #     with open('Desktop/IT/KARPOV_Courses/mars_cbr.xml', 'w') as csv_file:
    #         writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #         for Valute in root.findall('Valute'):
    #             NumCode = Valute.find('NumCode').text
    #             CharCode = Valute.find('CharCode').text
    #             Nominal = Valute.find('Nominal').text
    #             Name = Valute.find('Name').text
    #             Value = Valute.find('Value').text
    #             writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
    #                             [Name] + [Value.replace(",",".")])
    #             logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
    #                             [Name] + [Value.replace(",",".")])
    #
    #
    # xml_to_csv = PythonOperator(
    #     task_id='xml_to_csv',
    #     python_callable=xml_to_csv_func,
    #     dag=dag1
    # )
    #
    # def load_csv_to_greenplum_func():
    #     pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    #     pg_hook.copy_expert("COPY dina_cbr FROM STDIN DELIMETR ','", 'Desktop/IT/KARPOV_Courses/mars_cbr.xml')
    #
    # load_csv_to_greenplum = PythonOperator(
    #     task_id='load_csv_to_greenplum',
    #     python_callable=load_csv_to_greenplum_func
    #                                        )
    def postgrehook_conn_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        num_of_day = datetime(2022, 3, 1).isoweekday()
        cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        # one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        # logging.info(query_res[0])
        print(query_res[0])


    postgrehook_conn = PythonOperator(
        task_id='postgrehook_conn',
        python_callable=postgrehook_conn_func
    )

# export_cbr_xml >> xml_to_csv >> load_csv_to_greenplum
echo_ds1 >> postgrehook_conn
