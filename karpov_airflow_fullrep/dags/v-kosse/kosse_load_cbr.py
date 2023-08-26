"""
The DAG collects data from cbr and pushes it to Greenplum to table kosse_cbr.
Work from 01.03.2022 to 03.03.2022
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 4),
    'owner': 'v-kosse',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'poke_interval': 60,
    'sla': timedelta(hours=1)
}

file_xml = 'kosse_cbr.xml'
file_csv = 'kosse_cbr.csv'
dag_run_date = "{{ dag_run.logical_date.strftime('%d/%m/%Y') }}"


with DAG("kosse_load_cbr",
         schedule_interval='0 10 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['v-kosse']
         ) as dag:
    remove_old_files = BashOperator(
        task_id='remove_old_files',
        bash_command=f'rm -f /tmp/{file_csv} rm -f /tmp/{file_xml}'
    )


    def load_cbr_xml_func():
        return f'''curl https://www.cbr.ru/scripts/XML_daily.asp?date_req={dag_run_date} | 
        iconv -f Windows-1251 -t UTF-8 > /tmp/{file_xml} '''


    load_cbr_xml = BashOperator(
        task_id='load_cbr_xml',
        bash_command=load_cbr_xml_func()
    )


    def export_xml_to_csv_func(t1, t2, t3, dag_run_logical_date_run_date):
        logging.info(f'{t1} - date ds, {type(t1)} - date type ds')
        logging.info(f'{t2} - date logical, {type(t2)} - date type logical')
        logging.info(f'{t3} - date execution, {type(t3)} - date type execution')
        logging.info(f'{dag_run_logical_date_run_date} - date logical, '
                     f'{type(dag_run_logical_date_run_date)} - date type logical')

        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse(f'/tmp/{file_xml}', parser=parser)
        root = tree.getroot()

        with open(f'/tmp/{file_csv}', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for valute in root.findall('Valute'):
                num_code = valute.find('NumCode').text
                char_code = valute.find('CharCode').text
                nominal = valute.find('Nominal').text
                name = valute.find('Name').text
                value = valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [valute.attrib['ID']] + [num_code] + [char_code] + [nominal] +
                                [name] + [value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [valute.attrib['ID']] + [num_code] + [char_code] + [nominal] +
                             [name] + [value.replace(',', '.')])


    export_xml_to_csv = PythonOperator(
        task_id='export_xml_to_csv',
        python_callable=export_xml_to_csv_func,
        op_args=["{{ ds }}", "{{ dag_run.logical_date }}", "{{ execution_date }}",
                 "{{ dag_run.logical_date.strftime('%d/%m/%Y') }}"]
    )

    check_csv = BashOperator(
        task_id='check_csv',
        bash_command='ls -la /tmp | grep kosse'
    )


    def delete_old_data_func(date_db):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        sql = f"""delete 
                from kosse_cbr
                where dt = '{date_db}';
                """
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logging.info(f'Data from {date_db} successfully deleted')


    delete_old_data = PythonOperator(
        task_id='delete_old_data',
        python_callable=delete_old_data_func,
        op_args=["{{ dag_run.logical_date.strftime('%d.%m.%Y') }}"]
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum')
        # pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write') - connect to DB "Students"!
        pg_hook.copy_expert("COPY kosse_cbr FROM STDIN DELIMITER ','", f'/tmp/{file_csv}')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

remove_old_files >> load_cbr_xml >> export_xml_to_csv >> check_csv >> delete_old_data >> load_csv_to_gp