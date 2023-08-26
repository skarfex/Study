import logging
import json

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Connection
from airflow import settings


class ConnectionManagerOperator(BaseOperator):
    """Create or delete connections, according action param
        add - to create connection list
        del - to remove all created connections
    """

    def __init__(self,action: str, **kwargs):
        super().__init__(**kwargs)
        self.action = action
        self.conn_list=[]
        # Cbr.ru API
        self.conn_list.append(Connection(conn_id='cbr_http_conn',
                                         conn_type="http",
                                         host='http://cbr.ru/DailyInfoWebServ/DailyInfo.asmx'))
        #vk s3 bucket
        self.conn_list.append(Connection(conn_id='vk_S3_conn',
                                         conn_type="S3",
                                         host="https://hb.bizmrg.com",
                                         extra={"aws_access_key_id": "e4AxjhuF3gfkmuDDsCVWY1",
                                                "aws_secret_access_key": "eM7yowwQaDCLyFG6TzNW4iGCNnGixUeHzAfwxWHj3CBK",
                                                "host": "https://hb.bizmrg.com"}))


        params = {"conn_type": "postgres",
                  "host": "greenplum.lab.karpov.courses",
                  "login": "student",
                  "password":"Wrhy96_09iPcreqAS",
                  "schema": "karpovcourses",
                  "port": 6432,
                  "extra": ""}
        self.conn_list.append(Connection(conn_id='gp_kc', description="f*king connection",**params))


    def add_connections(self):
        for conn in self.conn_list:
            self.add_connection(conn)

    def del_connections(self):
        for conn in self.conn_list:
            self.del_connection(conn)

    def add_connection(self, conn):
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if str(conn_name) == str(conn.conn_id):
            logging.warning(f"Connection {conn.conn_id} already exists")
        else:
            session.add(conn)
            logging.info('Connection was created')
        logging.info(Connection.log_info(conn))

    def del_connection(self, conn):
        session = settings.Session()
        conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if str(conn_name) == str(conn.conn_id):
            logging.info('Connection was deleted')
            session.delete(conn_name)

    def execute(self, context):
        if self.action=='add':
            self.add_connections()
        elif self.action=='del':
            self.del_connections()
        else:
            logging.error(f'Wrong Connection Manager parameter ({self.action})')
            raise AirflowException(f'Wrong Connection Manager parameter ({self.action})')
