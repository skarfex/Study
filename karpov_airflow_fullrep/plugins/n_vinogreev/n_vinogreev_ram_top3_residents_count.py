from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import pandas as pd


class NVinogreevRAMHook(HttpHook):
    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'  # устанавливаем метод, с помощью которого будет проводиться запрос

    def get_count_of_pages(self):
        """Returns count of pages in location"""
        return self.run(endpoint='api/location').json()['info']['pages']

    def get_result_of_locations(self, page_num: str) -> list:
        """Returns descriptive data of location"""
        return self.run(endpoint=f'api/location/?page={page_num}').json()['results']


class NVinogreevRAMResidentsTopLocationsOperator(BaseOperator):
    """
    Get top locations based on their count of residents
    """

    def __init__(self, number_of_top: int, http_conn_id: str, postgres_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.number_of_top = number_of_top
        self.http_conn_id = http_conn_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        http_hook = NVinogreevRAMHook(self.http_conn_id)
        postgres_hook = PostgresHook(self.postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        # Получаем количество страниц
        result_count_of_pages = http_hook.get_count_of_pages()
        df = []
        for page in range(result_count_of_pages):
            # Стучимся в API и забираем результаты
            results = http_hook.get_result_of_locations(str(page + 1))
            for result in results:
                # Добавляем в количество резидентов в локации
                result['resident_cnt'] = len(result['residents'])
                df.append(result)

        df = pd.DataFrame.from_records(df)
        # Забираем только нужные столбцы
        df = df[['id', 'name', 'type', 'dimension', 'resident_cnt']]
        # Вычисляем топ локаций по резидентам
        df = df.nlargest(self.number_of_top, 'resident_cnt')

        logging.info(df.head())

        # Записываем данные в таблицу
        df.to_sql(
                con=engine,
                name='n_vinogreev_ram_location',
                schema='public',
                if_exists='replace',
                index=False
        )

        logging.info('Successfully inserted data to table')
