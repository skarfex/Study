import logging
import requests

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from airflow.models.xcom import XCom


class Ra_Valiev_Search_Loc_Operator(BaseOperator):
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        
    def get_pages_count(self):
        """
        This function to get count of pages from RaM API
        """
        api_url = "https://rickandmortyapi.com/api/location"
        response = requests.get(api_url)
        if response.status_code == 200:
            page_count = response.json()['info']['pages']
            logging.info(f'page count = {page_count}')
            return page_count
        else:
            logging.warning(f'Http status code is {response.status_code}')
            raise AirflowException("Warning: Can't get info from Rick and Morty API")
    
    def get_locations_count(self):
        
        results_final_list = []
        for page in range(1, self.get_pages_count()+1):
            page_url = f'https://rickandmortyapi.com/api/location?page={page}'
            response = requests.get(page_url)
            if response.status_code == 200:
                location_results = response.json()['results']
                for result in range(len(location_results)):
                    page_results_dict = location_results[result]
                    resident_count_dict = {'residents_count': len(page_results_dict['residents'])}
                    page_results_dict.update(resident_count_dict)
                    results_final_list.append(page_results_dict)
            else:
                logging.warning(f'Http status code is {response.status_code}')
                raise AirflowException("Warning: Can't get info from Rick and Morty API")
        return results_final_list
        
    def execute(self, context):
        results_final_list = self.get_locations_count()
        residents_count = []
        for dic in results_final_list:
            residents_count.append(dic['residents_count'])
        sorted_list = sorted(residents_count)
        top_three_count = sorted_list[-3:]
        top_three_list = []
        for dic in results_final_list:
            if dic['residents_count'] in top_three_count:
                top_three_list.append(dic)
        for loc in  top_three_list:
            del loc['residents']
            del loc['url']
            del loc['created']
        context['ti'].xcom_push(value=top_three_list, key='top_three_list')
        logging.info(f'Top 3 list of locations by residents_cnt is on xcom menu')