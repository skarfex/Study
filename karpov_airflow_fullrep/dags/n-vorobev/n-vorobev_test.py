from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'n-vorobev',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(year=2022, month=9, day=6, hour=0, minute=0),
    'end_date': datetime(year=2022, month=9, day=8, hour=0, minute=0)
}

with DAG("n-vorobev-test",
    schedule_interval='0 0 20 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-vorobev-2']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
    )

    def PrintLocalTimeZone():
        LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo

        return(LOCAL_TIMEZONE)

    local_time_zone = PythonOperator(
        task_id='local_time_zone',
        python_callable=PrintLocalTimeZone,
    )

    dummy >> echo_ds >> local_time_zone

