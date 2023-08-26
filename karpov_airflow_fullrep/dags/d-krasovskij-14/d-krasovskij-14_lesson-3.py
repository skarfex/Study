"""
Call trending tokens from Coingecko API
and call GP table 'articles'
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import requests
import json
import logging
from datetime import datetime, timedelta
import calendar

DEFAULT_ARGS = {
    'owner': 'd-krasovskij-14',
    'poke_interval': 300,
	'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 1),
    'end_date': datetime(2022, 11, 30),
    # 'schedule_interval': '*/15 * * * 1-6',
    'schedule_interval': '0 2 * * 1-6',
}

COINGECKO_URL = 'http://api.coingecko.com/api/v3'
TRENDING_ENDP = 'search/trending'
COINS_ENDP = 'coins'

bitcoin_id = 'bitcoin'


### aux functions

def call_articles_gp_table_func(date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    # ds = kwargs['ds']
    logging.info(date)
    calendar.setfirstweekday(calendar.MONDAY)
    date = datetime.fromisoformat(date)
    weekday = calendar.weekday(date.year, date.month, date.day) + 1 # counts from 0
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT heading FROM articles WHERE id = {weekday}', )
            data = cur.fetchone()
    logging.info(f"Weekday: {weekday}, Heading is: {data[:10]}")

def coin_pretty(x: dict): 
    return f"No. {x['trend']} Token {x['name']} ({x['sym']}) - {x['price']} USD"


def log_trending_coins_func(**kwargs):
    url_trends = '/'.join([COINGECKO_URL, TRENDING_ENDP])
    url_btc_price = '/'.join([COINGECKO_URL, COINS_ENDP, bitcoin_id])

    # logging.info("DAG execution date (from XCom): {}".format(
	# 	kwargs['ti'].xcom_pull(key='date', task_ids=id_call_articles_table)
	# ))

    ts_now = datetime.utcnow()

    try:
        logging.info('try to call BTC price...')
        res_btc = requests.get(url_btc_price)
        btc_price = json.loads(res_btc.content)\
                                ['market_data']\
                                ['current_price']\
                                ['usd']
        logging.info(f'try to process response (BTC price {btc_price} USD)')

        coins = requests.get(url_trends)
        coins = json.loads(coins.content)['coins']
        coins = [{'name': c['item']['name'],
                  'sym': c['item']['symbol'].upper(),
                  'trend': c['item']['score'] + 1,
                  'price': round(c['item']['price_btc'] * btc_price, 
                                 ndigits=3)} for c in coins]

        logging.info(f'{ts_now}\n' + '\n'.join([coin_pretty(c) for c in coins]))

    except Exception as e:
        logging.error(e)
        coins = None
    if coins:
        logging.info('valid coins')
        return 'dummy_good' 
    else: 
        logging.info('invalid coins')
        return 'dummy_bad'
	

### dag
with DAG(dag_id="d-krasovskij-14_lesson-3",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-krasovskij-14', 'lesson-4']) as dag:

    start = DummyOperator(task_id='start')

    echo_bash = BashOperator(
        task_id='id_echo_bash',
        bash_command='echo "ds={{ ds }}"',
        # do_xcom_push=True
    )

    call_articles_gp_table = PythonOperator(
        task_id='id_call_articles_table',
        python_callable=call_articles_gp_table_func,
        op_args=['{{ ds }}']
	)

    trending_coins = BranchPythonOperator(
        task_id='trending_coins',
        python_callable=log_trending_coins_func,
    )

    dummy_good = DummyOperator(task_id='dummy_good')
    dummy_bad = DummyOperator(task_id='dummy_bad')

    end = DummyOperator(task_id='end')

    start >> echo_bash >> call_articles_gp_table >> trending_coins >> [dummy_good, dummy_bad] >> end