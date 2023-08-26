# """
# —— Техническое задание ——
# 1. Создать таблицу в базе данных
# 2. Считать данные с API
# 3. Загрузить данные в созданную таблицу на GP

# —— Инструментарий ——
# 1. Поднятая база данных GreenPlum
# 2. Ссылка на API центрального банка России по изменениям курсов валют
#     2.1 https://cbr.ru/scripts/xml_daily.asp?date_req=05/12/2021

# """

# import csv
# import logging
# import xml.etree.ElementTree as ET

# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import PythonOperator


# DEFAULT_ARGS = {
#     'start_date': days_ago(2),
#     'owner': 's.dipner-7',
#     'poke_interval': 600
# }

# # Обозначаем константы
# execution_date = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d/%m/%Y") }}'
# url = "https://cbr.ru/scripts/xml_daily.asp?date_req={0}".format(execution_date)
# xml_filepath = "/tmp/semyon_cbr.xml"
# csv_filepath = "/tmp/semyon_cbr.csv"
# load_cbr_xml_script = f'''
# curl {url} | iconv -f Windows-1251 -t UTF-8 > {xml_filepath}
# '''

# dag = DAG("sdipner_load_cbr",
#         schedule_interval="@daily",
#         default_args=DEFAULT_ARGS,
#         max_active_runs=1,
#         tags=['karpov', 's.dipner-7']
#         )


# load_cbr_xml = BashOperator(task_id="load_cbr_xml",
#     bash_command=load_cbr_xml_script,
#     dag=dag
# )



    




