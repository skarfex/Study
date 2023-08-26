import logging
from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


XML_BODY="""<?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
            <GetCursOnDateXML xmlns="http://web.cbr.ru/">
            <On_date>dateTime</On_date>
            </GetCursOnDateXML>
            </soap:Body>
            </soap:Envelope>"""


PATH='/tmp/'

HEADERS={'Content-Type': 'text/xml; charset=utf-8',
         'SOAPAction': 'http://web.cbr.ru/GetCursOnDateXML'}

class CbrCurrencyHook(HttpHook):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.method='POST'
        self.http_conn_id='cbr_http_conn'

    def get_currency(self, onDate):
        xml_body = (XML_BODY.replace('dateTime', onDate))
        r= self.run('', data=xml_body, headers=HEADERS)
        return r

class VKS3Hook(S3Hook):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id='vk_S3_conn'
        self.bucket_name = 'd-bokarev-bucket'
        self.wildcard_match = True

class CbrCurrencyRateOperator(BaseOperator):
    def __init__(self, onDate: datetime = datetime.now(), **kwargs):
        super().__init__(**kwargs)
        self.onDate = onDate.strftime('%Y-%m-%d')
        self.xml_file_name = PATH+f"cours_{onDate.strftime('%Y-%m-%d')}.xml"
        self.parquet_file_name=f"cours_{onDate.strftime('%Y-%m-%d')}.parquet"

    def execute(self, context):

        #xml_body = (XML_BODY.replace('dateTime', self.onDate))
        #r = requests.post(URL, data=xml_body, headers=HEADERS)
        hook = CbrCurrencyHook()
        r=hook.get_currency(self.onDate)
        if r.status_code == 200:
            with open(self.xml_file_name, 'w') as file:
                file.write(r.text)
                logging.info(f"{self.xml_file_name} successfully saved")
            self.to_parquet()
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error occurred by getting currency rates')

    def to_parquet(self):
        try:
            root = ET.parse(self.xml_file_name).getroot()
            currency_list = []
            for valute in root.iter('ValuteCursOnDate'):
                currency_list.append([valute.find('Vname').text.strip(),
                                      valute.find('Vcode').text,
                                      valute.find('Vnom').text,
                                      valute.find('Vcurs').text
                                      ])
            currency_df = pd.DataFrame(data=currency_list, columns=['Name', 'Code', 'Nom', 'Rate'])
            currency_df.to_parquet(self.parquet_file_name, index=False)
            logging.info(f"{self.parquet_file_name} successfully saved")

            hook =VKS3Hook()
            hook.load_file(filename=self.parquet_file_name, key=self.parquet_file_name, bucket_name=hook.bucket_name, replace=True)
            logging.info(f"{self.parquet_file_name} successfully pulled to S3")
        except Exception:
            raise AirflowException('Error occurred by creating parquet')
