import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.xcom import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from ra_valiev_plugins.ra_valiev_weather_op import Ra_Valiev_Weather_Operator


DEFAULT_ARGS = {
    'start_date': days_ago(0),
    'owner':  'ra-valiev',
    'poke_interval': 600
}

with DAG ('ra_valiev_weather_DAG',
    schedule_interval='00 7,19 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ra-valiev']
 ) as dag:
    
    start_task = DummyOperator(task_id='start')
    
    get_weather_task = Ra_Valiev_Weather_Operator(
        task_id='get_weather_task'
    )
    
    def show_weather_task_func(**kwargs):
        get_weather_list = kwargs['ti'].xcom_pull(task_ids='get_weather_task', key='weather_list')
        logging.info(f"Погода в городе {get_weather_list[0]['city']}, {get_weather_list[1]['country']} на момент {get_weather_list[2]['local_time']} (обновление данных в {get_weather_list[3]['last_updated']}): ")
        logging.info(f"Температура {get_weather_list[4]['temp_c']}°C (ощущается, как {get_weather_list[10]['feelslike']} °C), {get_weather_list[5]['condition']}")
        logging.info(f"Скорость ветра {get_weather_list[6]['wind_kph']} км/ч, с порывами до {get_weather_list[7]['gust_kph']} км/ч")
        logging.info(f"Влажность воздуха {get_weather_list[8]['humidity']} %")
        logging.info(f"Облачность {get_weather_list[9]['cloudness']}")
        logging.info(f"Видимость {get_weather_list[11]['visible']} км")
    
    show_weather_task = PythonOperator(
        task_id='show_weather_task',
        python_callable=show_weather_task_func
    )
    
    
    end_task = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )
    
start_task >> get_weather_task >> show_weather_task >> end_task