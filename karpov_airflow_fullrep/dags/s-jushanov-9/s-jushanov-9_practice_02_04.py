"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import csv
import xml.etree.ElementTree as ET

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-jushanov-9',
    'poke_interval': 600
}

# TODO: вынести url, файлы с xml и csv в константу
url = "https://www.cbr.ru/scripts/XML_daily.asp?date_req=01/11/2021"
filename_xml = "/tmp/cbr.xml"
filename_csv = "/tmp/cbr.csv"


dag = DAG(
    "s_jushanov_02_04_practice",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jushanov-9']
)

# TODO: Не константа, а {{ format_ds... }}
# TODO: удалять файл cbr.xml, если он уже есть
# load_cbr_xml_script = '''rm -f {file_xml} && curl {url} | iconv -f Windows-1251 -t UTF-8 > {file_xml}'''

load_cbr_xml = BashOperator(
    task_id='load_cbr_xml',
    bash_command=f'rm -f {filename_xml} && curl {url} | iconv -f Windows-1251 -t UTF-8 > {filename_xml}',
    dag=dag
)


def export_xml_to_csv_func():
    parser = ET.XMLParser(encoding="UTF-8")
    tree = ET.parse(filename_xml, parser=parser)
    root = tree.getroot()

    with open(filename_csv, 'w') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
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
    python_callable=export_xml_to_csv_func,
    dag=dag
)


def load_csv_to_gp_func():
    pg_hook = PostgresHook('conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # TODO: Мёрдж или очищение предыдущего батча
    cursor.execute(
        f"""
        CREATE TEMP TABLE s_jushanov_9_tmp_cbr
        (LIKE s_jushanov_9_cbr INCLUDING DEFAULTS)
        DISTRIBUTED BY (dt);

        COPY s_jushanov_9_tmp_cbr FROM '{filename_csv}' WITH DELIMITER ',';

        INSERT INTO s_jushanov_9_cbr
        SELECT *
        FROM s_jushanov_9_tmp_cbr
        ON CONFLICT DO NOTHING;

        DROP TABLE s_jushanov_9_tmp_cbr;
        """
    )
    cursor.close()
    # logging.info('Temp table create')
    # pg_hook.copy_expert("COPY s_jushanov_9_tmp_cbr FROM STDIN DELIMITER ','", filename_csv)
    # cursor.execute(
    #     """
    #     INSERT INTO s_jushanov_9_cbr
    #     SELECT *
    #     FROM s_jushanov_9_tmp_cbr
    #     ON CONFLICT DO NOTHING;
    #     """
    # )
    # logging.info('Insert into table')
    # cursor.execute(
    #     """
    #     DROP TABLE s_jushanov_9_tmp_cbr;
    #     """
    # )
    # logging.info('Drop temp table')


load_csv_to_gp = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_to_gp_func,
    dag=dag
)


load_cbr_xml >> export_xml_to_csv >> load_csv_to_gp
