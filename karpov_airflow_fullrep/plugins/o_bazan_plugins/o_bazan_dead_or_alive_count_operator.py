'''
Оператор для подсчета живых или мертвых персонажей
'''

import logging

from airflow.models import BaseOperator
from o_bazan_plugins.o_bazan_rick_and_morty_hook import OBazanRickMortyHook

class OBazanDeadOrAliveCountOperator(BaseOperator):
    """
    Count number of dead or alive characters
    by OBazanRickMortyHook
    """

    template_fields = ('dead_or_alive',)
    ui_color = "#c7ffe9"

    # Определение инициализатора класса. dead_or_alive: str означает, что при вызове оператора будет ожидаться
    # флаг Dead/Alive (по умолчанию Dead)
    def __init__(self, dead_or_alive: str = 'Dead', **kwargs) -> None:
        super().__init__(**kwargs)
        self.dead_or_alive = dead_or_alive

    def get_dead_or_alive_count_on_page(self, result_json: list) -> int:
        """
        Get count of dead or alive character in one page
        :param result_json (List of dictionaries. Dictionary consists info about one character)
        :return: dead_or_alive_count in one page
        """
        dead_or_alive_count_on_page = 0
        for one_char in result_json:
            if one_char.get('status') == self.dead_or_alive:
                dead_or_alive_count_on_page += 1
        logging.info(f'{self.dead_or_alive} count_on_page = {dead_or_alive_count_on_page}')
        return dead_or_alive_count_on_page

    def execute(self, context):
        """
        Logging count of dead or alive in Rick&Morty API
        :param
        :return: dead_or_alive_count in all pages
        """
        hook = OBazanRickMortyHook('dina_ram') # создаем экземпляр хука и определеяем conn_id (dina_ram), который
        # должнен быть задан администратором в airflow
        dead_or_alive_count = 0
        for page in range(hook.get_char_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_char_page(str(page + 1))
            dead_or_alive_count += self.get_dead_or_alive_count_on_page(one_page)
        logging.info('________________________________________________________________')
        logging.info(f'{self.dead_or_alive} in Rick&Morty: {dead_or_alive_count}')
        logging.info('________________________________________________________________')