'''
Оператор, посвященный заданию 5-го урока из блока "ETL"
Оператор возвращает из API Rick&Morty ТОП-N локаций по количеству резидентов
'''

import logging

from airflow.models import BaseOperator
from o_bazan_plugins.o_bazan_ram_hook import OBazanRAMHook


class TOPLocationOperatorWIthHook(BaseOperator):

    template_fields = ('top_num',) # определяем кортеж объектов, которые могут быть шаблонизированы (заданы в таске)
    ui_color = "#ffa3c5" # задаем цвет оператора

    def __init__(self, top_num: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.top_num = top_num

    # Фильтрация ТОП-N локаций по количеству резидентов
    def execute(self, context):
        """
        Get info about TOP-top_num locations by count of residents
        :param
        :return species_count (list of tuples that contain info about each location in API)
        """
        hook = OBazanRAMHook('dina_ram')
        location_info = hook.get_location_info()

        # Проверка, что top_num меньше или равно, чем общее кол-во локаций
        if self.top_num > len(location_info):
            raise ValueError("The specified top_num is greater than the total number of locations.")

        top_location = sorted(location_info, key=lambda x: x[4], reverse=True)
        logging.info(top_location[:self.top_num])
        return top_location[:self.top_num]