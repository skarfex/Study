"""
Получаем список всех коннекторов
"""

from airflow import DAG

import logging

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

from airflow.utils.dates import days_ago
from airflow import settings
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'poke_interval': 600,
    'owner': 'd-bokarev'
}

S3_CONNECTION_NAME = 'vk_conn_S3'
with DAG("d-bokarev_s3_conn",
         default_args=DEFAULT_ARGS,
         schedule_interval='@once',
         tags=['d-bokarev']) as dag:
    def add_connection(**context):
      #  s3con = Connection(conn_id=S3_CONNECTION_NAME,
       #                    conn_type="S3",
       #                    host = "https://storage.yandexcloud.net/",
       #                    extra= {"aws_access_key_id":"YCAJEuQh_xp3GFngCYcRDBjTQ",
       #                            "aws_secret_access_key": "YCP9U9GycSHbIAVebDPSBjs-fGpvZzUYUjB-uCN7",
       #                            "host": "https://storage.yandexcloud.net/"}
       #                   )
        s3conn = Connection(conn_id=S3_CONNECTION_NAME,
                            conn_type="S3",
                            host="https://hb.bizmrg.com",
                            extra={"aws_access_key_id": "e4AxjhuF3gfkmuDDsCVWY1",
                                   "aws_secret_access_key": "eM7yowwQaDCLyFG6TzNW4iGCNnGixUeHzAfwxWHj3CBK",
                                   "host": "https://hb.bizmrg.com"}
                            )
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == S3_CONNECTION_NAME).first()
        if str(conn_name) == str(S3_CONNECTION_NAME):
            logging.warning(f"Connection {S3_CONNECTION_NAME} already exists")
        else:
            session.add(s3conn)
            logging.info('Connection was created')
            logging.info(Connection.log_info(s3conn))

    def del_connection():
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == S3_CONNECTION_NAME).first()
        if str(conn_name) == str(S3_CONNECTION_NAME):
            logging.info('Connection was deleted')
            session.delete(conn_name)



    check_s3_sensor = S3KeySensor(task_id='s3_sensor',
                         bucket_name='d-bokarev-bucket',
                         bucket_key='*.csv',
                         aws_conn_id=S3_CONNECTION_NAME,
                         wildcard_match=True,
                         poke_interval=5,
                         timeout=30)


    add_conn = PythonOperator(
        task_id='add_connection',
        python_callable=add_connection,
        provide_context=True
    )
    del_conn = PythonOperator(
        task_id='del_connection',
        python_callable=del_connection,
        provide_context=True
    )

    add_conn>>check_s3_sensor>>del_conn