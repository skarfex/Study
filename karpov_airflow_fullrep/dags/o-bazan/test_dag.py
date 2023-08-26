"""
Урок 2. Тестовый даг
"""

# Импорт библиотек
from airflow import DAG  # импорт пакетов функции DAG
from airflow.utils.dates import days_ago # импорт функции для работы со временем
import logging # импорт библиотеки для логирования

# Импорт операторов
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# Определение дефолтных аргументов в словаре
DEFAULT_ARGS = {
    'start_date': days_ago(2), # дата первого запуска дага
    'owner': 'o-bazan', # владелец дага
    'poke_interval': 600 # время в секундах между каждой попыткой запуска
}

# Определение основных метаданных дага
with DAG("test_dag", # имя дага
    schedule_interval='@daily', # расписание
    default_args=DEFAULT_ARGS,
    max_active_runs=1, # определяет, сколько параллельно запущенных инстансов разрешено
    tags=['ob_test', 'o-bazan'] # тэги
) as dag:

    # Блок описания функций
    def hello_world_func():
        logging.info("Hello World!") # вывод текста в лог

    # Блок описания тасок
    dummy_task = DummyOperator(task_id="dummy_task") # оператор бездействия

    echo_task = BashOperator(
        task_id='echo_task',
        bash_command='echo {{ ds }}' # вывод даты запуска даг инстанса
    )

    hello_world_task = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world_func
    )

    dummy_task >> [echo_task, hello_world_task]