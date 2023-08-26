

import logging

from airflow.models import BaseOperator
from el_sergeeva_plugins.el_sergeeva_hook import El_sergeeva_hook
import pandas
class El_sergeeva_operator (BaseOperator):

    template_fields = ('count_top',)
    ui_color = "#c7ffe9"


    def __init__(self,count_top: int = 3,**kwargs)->None:
        super().__init__(**kwargs)
        self.count_top =count_top


    def get_list_location_on_page(self, result_json: list) ->list:
        """
        Get count list of top location
        """

        locations_list = []
        for one_location in result_json:
            locations_list.append(
                {"id": one_location['id'],
                 "name": one_location['name'],
                 "type": one_location['type'],
                 "dimension": one_location['dimension'],
                 "residents_cnt": len(one_location['residents'])}
            )

        return locations_list

    def top_location_list(self,location_list: list) ->list:
        df_locations_list = pandas.DataFrame(location_list)
        topLocations = df_locations_list.nlargest(self.count_top, 'residents_cnt')
        topLocations_list = topLocations.values.tolist()
        return topLocations_list
    def execute(self, context):
        """
        Get count list of top location
        """
        list_location=[]
        hook = El_sergeeva_hook('dina_ram')
        dead_or_alive_count = 0
        for page in range(hook.get_location_page_count()):
            logging.info(f'PAGE {page + 1}')
            one_page = hook.get_location_page(str(page + 1))
            list_location += self.get_list_location_on_page(one_page)
        topLocations=self.top_location_list(list_location)
        logging.info(f'Best  {self.count_top }  location in Rick&Morty {topLocations}')
        return topLocations
