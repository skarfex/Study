from airflow.sensors.base import BaseSensorOperator
import random
import logging


class NChepurnyh19RandomSensor(BaseSensorOperator):
    """
    Sensor wait, when random number from 0 to range_number will be equal to zero
    """

    ui_color = '#fffacd'

    def __init__(self, range_number: int = 2, **kwargs) -> None:
        super().__init__(**kwargs)
        self.range_number = range_number

    def poke(self, context):
        """
        :return: if random number equal to zero
        """
        poke_num = random.randrange(0, self.range_number)
        logging.info(f'poke: {poke_num}')
        return poke_num == 0
