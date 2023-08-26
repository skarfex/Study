"""
Igor Batyukov

My first custom Airflow operator

"""

import logging
import csv
from collections import defaultdict
from i_batjukov_10_plugins.i_batjukov_10_ram_locations_hook import IBatjukov10RamLocationsHook
from airflow.models.baseoperator import BaseOperator
from typing import Sequence


class IBatjukov10RamLocationsOperator(BaseOperator):

    """
    Returns csv file with selected number of locations with max residents in each of them
    """

    ui_color = '#e0ffff'
    template_fields: Sequence[str] = ("execution_dt",)

    def __init__(self, execution_dt: str, num_of_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.num_of_locations = num_of_locations
        self.locations_dict = defaultdict(int)
        self.execution_dt = execution_dt

    def execute(self, context):
        """
        The function uses hook to interact with API.
        For each location on page counts amount of residents and adds that value with relevant location id in dict.
        Details of the selected number of locations filled in csv file then
        """

        csv_path = '/tmp/ram.csv'
        hook = IBatjukov10RamLocationsHook('dina_ram')
        for page in range(hook.get_page_count()):
            for location in hook.get_locations_on_page(str(page + 1)):
                self.locations_dict[location.get('id')] = len(location.get('residents'))
                logging.info(f"Location id:{location.get('id')} / Number of residents:{len(location.get('residents'))}")

        with open(csv_path, 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            logging.info(f'Top {self.num_of_locations} locations with max number of residents')
            for num in range(self.num_of_locations):
                loc_id = sorted(self.locations_dict.items(), key=lambda x: x[1], reverse=True)[num][0]
                json_data = hook.get_final_json(str(loc_id))
                writer.writerow(
                    [self.execution_dt] + [json_data['id']] + [json_data['name']] +
                    [json_data['type']] + [json_data['dimension']] + [len(json_data['residents'])]
                )
                logging.info(f'----------Location No. {num}------------')
                logging.info([json_data['id']] + [json_data['name']] +
                             [json_data['type']] + [json_data['dimension']] + [len(json_data['residents'])])
                logging.info('-----------------------------------------')
