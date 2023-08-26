import logging
import requests

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class EgLebedevRamOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, **kwargs):

        r = requests.get("https://rickandmortyapi.com/api/location")
        
        def sortBy(location):
            logging.info(location)
            return len(location.get('residents'))

        js = r.json()
        logging.info(js)
        rss = js.get('results')
        rss.sort(key=sortBy, reverse=True)
        logging.info(rss)
        rss3 = rss[:3]
        logging.info("locs: {}".format(rss3))
        sql = ''
        
        hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            for l in rss3:
                sql += "('{}', '{}', '{}', {}),"
            pars = ()
            for l in rss3:
                pars += (l.get('name'), l.get('type'), l.get('dimension'), str(len(l.get('residents'))))
            
            sqql = "insert into eg_lebedev_ram_location (name, type, dimension, resident_cnt) values " + sql[:len(sql)-1] + ";"
            
            logging.info(sqql)
            logging.info(pars)
            ress = sqql.format(*pars)
            logging.info(ress)

            cursor.execute(ress)
        conn.commit()
