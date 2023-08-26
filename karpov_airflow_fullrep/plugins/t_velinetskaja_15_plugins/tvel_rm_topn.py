"""
Записываем топ N локаций по количеству резидентов в таблицу
"""
from ast import literal_eval
import logging
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class TVelRMHook(HttpHook):
    """
    Получаем количество страниц и данные из API
    """

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_loc_page_count(self):
        """Returns count of page in API"""
        return self.run('api/location').json()['info']['pages']

    def get_loc_page(self, page_num: str) -> list:
        """Returns count of page in API"""
        return self.run(f'api/location/?page={page_num}').json()['results']


class TVelRMTopNOperator(BaseOperator):
    """
    Записываем в GP топ N строк по кол-ву резидентов
    """

    template_fields = ('n','tbl_nm',)
    ui_color = "#c7ffe9"

    def __init__(self, n: int = 3, tbl_nm: str = 't_velinetskaja_15_ram_location', **kwargs) -> None:
        super().__init__(**kwargs)
        self.n = n
        self.tbl_nm = tbl_nm

    def get_top_locations(self, resfromhook):
        loc_dict = {}
        for page in range(resfromhook.get_loc_page_count()):
            page_res = resfromhook.get_loc_page(page + 1)
            for res in page_res:
                str_val = str({"id":res["id"], "name":res["name"], "type":res["type"], "dimension":res["dimension"]})
                loc_dict[str_val] = len(res["residents"])
        loc_dict = {k:v for k,v in sorted(loc_dict.items(), key=lambda kv: kv[1], reverse=True)[:self.n]}
        logging.info(f'Получено ТОП-{self.n} локаций с максимальным количеством резидентов')
        return loc_dict

    def clear_table(self, connhook):
        connhook.run(f'DELETE FROM {self.tbl_nm};', False)
        logging.info(f'Удалены старые записи из таблицы {self.tbl_nm}')

    def execute(self, context):
        resfromhook = TVelRMHook('dina_ram')
        connhook = PostgresHook('conn_greenplum_write')
        result = self.get_top_locations(resfromhook)
        self.clear_table(connhook)
        for k in result:
            row_dict = literal_eval(k)
            SQL = f"""INSERT INTO {self.tbl_nm} 
                        SELECT {row_dict['id']} as id, '{row_dict['name']}' as name, '{row_dict['type']}' as type, 
                               '{row_dict['dimension']}' as dimension, {result[k]} as resident_count;"""
            connhook.run(SQL, False)
            logging.info(f'Добавлена строка {k} с количеством резидентов {result[k]} в таблицу {self.tbl_nm}')
