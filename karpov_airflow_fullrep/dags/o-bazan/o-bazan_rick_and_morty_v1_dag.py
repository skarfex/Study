'''
Даг для подсчета количества людей в сериале (реализация без написания кастомного оператора)
'''


from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
import logging
import requests

from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id = "o-bazan_rick_and_morty_v1_dag",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    tags=['lesson5', 'Rick and Morty', 'o-bazan']
)

# Подсчет количества страниц
def get_page_count(api_url):
    """
    Get count of page in API
    :param api_url
    :return: page count
    """
    r = requests.get(api_url)
    if r.status_code == 200:
        logging.info("SUCCESS")
        page_count = r.json().get('info').get('pages')
        logging.info(f'page_count = {page_count}')
        return page_count
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))
        raise AirflowException('Error in load page count')

# Подсчет количества людей на странице
def get_human_count_on_page(result_json):
    """
    Get count of human in one page of character
    :param result_json
    :return: human count
    """
    human_count_on_page = 0
    for one_char in result_json:
        if one_char.get('species') == 'Human':
            human_count_on_page += 1
    logging.info(f'human_count_on_page = {human_count_on_page}')
    return human_count_on_page

# Вывод количества людей во всем сериале
def load_ram_func():
    """
    Logging count of Human in Rick&Morty
    """
    human_count = 0
    ram_char_url = 'https://rickandmortyapi.com/api/character/?page={pg}'
    for page in range(get_page_count(ram_char_url.format(pg='1'))):
        r = requests.get(ram_char_url.format(pg=str(page + 1)))
        if r.status_code == 200:
            logging.info(f'PAGE {page + 1}')
            human_count += get_human_count_on_page(r.json().get('results'))
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load from Rick&Morty API')
    logging.info(f'Humans in Rick&Morty: {human_count}')


count_num_humans = PythonOperator(
    task_id='count_num_humans',
    python_callable=load_ram_func,
    dag=dag
)

count_num_humans