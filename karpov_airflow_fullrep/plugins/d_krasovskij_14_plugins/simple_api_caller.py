"""'Universal' API caller"""

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

from d_krasovskij_14_plugins.db_utils import generate_upsert_statement, prepare_values
from d_krasovskij_14_plugins.utils import filter_result

import logging


# def ram_api_loc_page_url(page=1):
#     return f'https://rickandmortyapi.com/api/location?page={page}'

class SimpleApiCallerLoader(BaseOperator):
    """'Universal' API caller"""

    def __init__(self,
                 db_connection:str,
                 tablename:str,
                 endpoint:str, 
                 path:str, 
                 fetch_n:int,
                 fetch_by:str,
                 desc:bool,
                 pagination:bool, 
                 pages:int=None,
                 async_caller:bool=False,
                 **kwargs
                 ):
        super().__init__(**kwargs)

        self.db_connection = db_connection
        self.tablename = tablename
        self.endpoint = endpoint
        self.path = path
        self.n = fetch_n
        self.by = fetch_by
        self.desc = desc
        self.pagination = pagination
        self.n_pages = pages
        if async_caller:
            try:
                import asyncio
                import aiohttp
                import sys
                self.is_async=True

            except:
                logging.info("'aiohttp' library not registrated. use 'requests' library instead")
                import requests
                self.is_async=False
        else:    
            self.is_async=False

        self.result = []


    def load_from_api(self):
        if self.is_async:
            from d_krasovskij_14_plugins.async_caller import async_call
            result = async_call(self.endpoint, self.path, self.pagination, self.n_pages)
            self.result += result
        else:
            from d_krasovskij_14_plugins.sync_caller import sync_call
            result = sync_call(self.endpoint, self.path, self.pagination, self.n_pages)
            self.result += result

    def filter_result(self):
        self.result = filter_result(lst=self.result, 
                                    n=self.n,
                                    by=self.by,
                                    reverse=self.desc)

    def load_to_db(self):
        gp_hook = PostgresHook(postgres_conn_id=self.db_connection)

        values = prepare_values(self.result, in_braces=False)
        stmt = generate_upsert_statement(tablename=self.tablename, 
                                         values_list_str=values)

        with gp_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(stmt, False)
                    logging.info('VALUES INSERTED')
                except Exception as e:
                    raise AirflowException(f'INSERT FAILS. statement:\n{stmt}\n\nINTERCEPTED EXCEPTION:\n{e}')

    def execute(self, context):
        self.load_from_api()
        self.filter_result()
        self.load_to_db()
