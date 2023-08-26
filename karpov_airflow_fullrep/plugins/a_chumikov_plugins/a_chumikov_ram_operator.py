import requests
import logging
import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


class MaxNLocationsOperator(BaseOperator):
    """
    HTTP-запрос к API Rick&Morty, фильтрация и преобразование данных для
    нахождения n локаций сериала с наибольшим количеством резидентов. Далее
    отправка полученных строк в Greenplum в указанную таблицу."
    """

    ui_color = "#e0ffff"

    def __init__(self, api_url: str, table_name_to_insert: str,
                 top_n: int = 3, **kwargs) -> None:

        super().__init__(**kwargs)
        self.api_url = api_url
        self.top_n = top_n
        self.table_name_to_insert = table_name_to_insert

    @staticmethod
    def check_response(url: str, request_name: str):
        """
        Проверяет и логирует ответ сервера по HTTP-запросу.
        Если ответ успешен, то возвращает объект ответа, иначе AirflowException.
        """

        response = requests.get(url)
        if response:
            logging.info(f'SUCCESS RESPONSE FROM SERVER ON {request_name}')
            return response
        else:
            logging.warning(f'HTTP STATUS CODE = {response.status_code}')
            raise AirflowException(f'BAD RESPONSE FROM SERVER ON {request_name}')

    def get_number_of_pages(self) -> int:
        """
        Узнаёт число отдельных страниц с нужными данными по эндпоинту.
        """
        return MaxNLocationsOperator.check_response(self.api_url, 'Число страниц')\
            .json()['info']['pages']

    def get_api_data_and_process(self) -> dict:
        """
        Основной метод по забору данных из api, их обработка и конвертация в словарь
        """
        url_for_every_page = self.api_url + '?page={number}'
        main_df = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])  # основной датафрейм

        for i in range(self.get_number_of_pages()):  # нужно обработать каждую страницу
            response = MaxNLocationsOperator.check_response(url_for_every_page.format(number=i + 1), f'Запрос к странице №{i}')
            json_data = response.json()['results']  # берем нужные данные

            df = pd.DataFrame.from_records(json_data)  # создаём датафрейм по полученным данным
            df['resident_cnt'] = df.residents.str.len()  # добавляем нов. колонку с длинами списков residents
            df = df.drop(columns=['url', 'created', 'residents'])  # удаляем ненужн. колонки
            # во избежании side эффектов не использую inplace=True, где это возможно

            top_n_residents_loc = df.nlargest(self.top_n, 'resident_cnt', keep='all')  # делаем нужную выборку
            # делаю поиск top_n_residents_loc избыточно на каждой итерации, чтобы избавляться от ненужных строк сразу

            main_df = pd.concat([main_df, top_n_residents_loc])  # добавляем результат итерации к основному датафрейму

        main_df['resident_cnt'] = pd.to_numeric(main_df.resident_cnt)  # иначе nlargest не отработает, т.к. pd.concat всех object сделал
        main_df = main_df.nlargest(self.top_n, 'resident_cnt', keep='all')  # финаль. поиск 3-х локаций с наиб. кол-м резидентов

        dict_data = main_df.to_dict(orient='split')  # приводим данные обратно к словарю
        del dict_data['index']  # убираем ненужный ключ 'index', пришедший из датафрейма
        return dict_data

    def get_query_to_insert(self) -> str:
        """
        Формирует строку SQL запроса для вставки в базу данных из словаря
        """
        data = self.get_api_data_and_process()
        col_names = ', '.join(data['columns'])  # строка с колонками, используемая в total_query
        transform = (f'{tuple(el)}' for el in data['data'])  # промежуточн. преобразования
        values_to_insert = ', '.join(transform)
        return f'insert into {self.table_name_to_insert} ({col_names}) values {values_to_insert};'

    def execute(self, context) -> None:
        """Передаём SQL запрос к Greenplum"""

        pg_hook = PostgresHook('conn_greenplum_write')
        commands = ['truncate table ' + self.table_name_to_insert, self.get_query_to_insert()]
        pg_hook.run(commands, autocommit=True)

