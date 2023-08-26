"""
Operator and hook for Lesson 5
"""

import logging
import re

from typing import List

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook


class RickMortyHook(HttpHook):
    def __init__(self, **kwargs) -> None:
        super().__init__(http_conn_id="", **kwargs)
        self.method = 'GET'
        self.base_url = "https://rickandmortyapi.com/"
        self.available_resources = self._get_available_resources()

    def get_all_elements_from_resource(self, resource: str) -> List[dict]:
        """Возвращает все элементы ресурса"""
        if resource not in self.available_resources.keys():
            logging.error("Обращение к несуществующему ресурсу!")
            raise AirflowFailException
        resource_elements = []
        initial_endpoint = self._extract_endpoint(self.available_resources[resource])
        resp = self.run(initial_endpoint).json()
        resource_elements.extend(resp["results"])
        while resp["info"]["next"]:
            next_endpoint = self._extract_endpoint(resp["info"]["next"])
            resp = self.run(next_endpoint).json()
            resource_elements.extend(resp["results"])
        return resource_elements

    def _get_available_resources(self) -> dict:
        """Возвращает доступные ресурсы и урлы – точки входа в них"""
        return self.run("api").json()

    def _extract_endpoint(self, url: str) -> str:
        """Вырезает из адреса базовый URL и возвращает эндпоинт"""
        return re.sub(self.base_url, "", url)


class RMGetPopularLocationOperator(BaseOperator):
    def __init__(self, top_n: int, table: str, **kwargs):
        super().__init__(**kwargs)
        self.top_n = top_n
        self.table = table

    def execute(self, **kwargs):
        rm_hook = RickMortyHook()
        rm_locations = rm_hook.get_all_elements_from_resource("locations")

        for location in rm_locations:
            location["residents_cnt"] = len(location["residents"])
        top_locations = sorted(rm_locations, key=lambda loc: loc["residents_cnt"], reverse=True)[: self.top_n]
        if len(top_locations) > 0:
            sql_statement = self._get_insert_statement(top_locations)
        else:
            logging.error("Данные для вставки в таблицу не получены!")
            raise AirflowException

        return sql_statement

    def _get_insert_statement(self, data: List[dict]) -> str:
        """Функция получает на вход список словарей и возвращает INSERT запрос с данными из них"""
        values = []
        for elem in data:
            value = f"({elem['id']}, '{elem['name']}', '{elem['type']}', '{elem['dimension']}', {elem['residents_cnt']})"
            values.append(value)
        insert_statement = f"INSERT INTO {self.table} VALUES {', '.join(values)}"
        return insert_statement
