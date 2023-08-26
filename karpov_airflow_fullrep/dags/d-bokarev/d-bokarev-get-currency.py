import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from d_bokarev_plugins.d_bokarev_cbr_operator import CbrCurrencyRateOperator
from d_bokarev_plugins.d_bokarev_conn_manager_operator import ConnectionManagerOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

from airflow.models import Connection
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-bokarev',
    'poke_interval': 600
}

with DAG("d-bokarev-cbr-get-currency",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-bokarev']
         ) as dag:
    def pull_from_gp_func(**kwargs):
        hook =PostgresHook('gp_kc')
        logging.info(Connection.log_info(hook.get_connection('gp_kc')))
        conn=hook.get_conn()
        logging.info(Connection.log_info(hook.get_connection('conn_greenplum')))
        cursor = conn.cursor("cursor_name")
        cursor.execute(f'SELECT * FROM articles')
        results = cursor.fetchall()
        for res in results:
            logging.info(res)

    get_connections_in_stdout = BashOperator(task_id="get_connections_in_stdout",
                                             bash_command="airflow connections export -")


    s3sensor = S3KeySensor(task_id='s3_sensor',
                           bucket_name='d-bokarev-bucket',
                           bucket_key='*.parquet',
                           aws_conn_id="vk_S3_conn",
                           wildcard_match=True,
                           poke_interval=5,
                           mode='reschedule',
                           timeout=30)


   # spark_submit_mart = SparkSubmitOperator(task_id='submit_mart',
    #                                        application="d-bokarev-spark-job.py")

    add_connections = ConnectionManagerOperator(task_id='create_connections', action='add')
    cbr_get_currency2 = CbrCurrencyRateOperator(task_id = 'cbr_get_currency')
    delete_connections = ConnectionManagerOperator(task_id='delete_connections', action='del', trigger_rule='all_done')

    add_connections>>get_connections_in_stdout>>cbr_get_currency2>>s3sensor>>delete_connections

