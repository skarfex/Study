from lib2to3.pytree import Base
from airflow.models import BaseOperator
import requests
from airflow.exceptions import AirflowException
from operator import itemgetter
from airflow.hooks.postgres_hook import PostgresHook

class VBLocationsOperator(BaseOperator):
    
    def __init__(self, api_link: str, **kwargs):
        super().__init__(**kwargs)
        self.api_link = api_link

    def get_page_count(self, api_url):
        """
        Get count of page in API
        :param api_url
        :return: page count
        """
        r = requests.get(api_url)
        if r.status_code == 200:
            print("SUCCESS")
            page_count = r.json().get('info').get('pages')
            print(f'page_count = {page_count}')
            return page_count
        else:
            print("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')

    def get_ids_and_names(self, json_with_residents):
        """
        for page provided get list of all 
        location IDs with count of residents in each
        """
        list_of_resid = []
        for resid in json_with_residents:
            list_of_resid.append([resid.get("id"), len(resid.get("residents"))])

        return list_of_resid

    def get_all_location_info(self, location_id):
        location_info = requests.get(f"{self.api_link}/location/{location_id}")
        if location_info.status_code == 200:
            print(f"SUCCESS to get location#{location_id}")
            loc_name = location_info.json().get("name")
            loc_type = location_info.json().get("type")
            loc_dim = location_info.json().get("dimension")
            print(f'location info: loc_name = {loc_name}, loc_type = {loc_type}, loc_dim = {loc_dim}')
            return (loc_name, loc_type, loc_dim)
        else:
            print("HTTP STATUS {}".format(location_info.status_code))
            raise AirflowException('Error in load get_all_location_info')


    def load_ram_func(self):
        """    
        get list of all locations in [id, residents_count] format
        """
        human_count = 0
        ram_char_url = f'{self.api_link}/location/?page='+'{pg}'
        
        all_locations = []
        for page in range(self.get_page_count(ram_char_url.format(pg='1'))):
            r = requests.get(ram_char_url.format(pg=str(page + 1)))
            if r.status_code == 200:
                print(f'PAGE {page + 1}')
                all_locations.extend(self.get_ids_and_names(r.json().get('results')))
            else:
                print("HTTP STATUS {}".format(r.status_code))
                raise AirflowException('Error in load from Rick&Morty API')
        return all_locations   

    def execute(self, context):
        main_list = self.load_ram_func()
        main_list = sorted(main_list, key=itemgetter(1), reverse=True)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        
        # Delete data before insert
        sql_statement = f"""
        DELETE FROM V_BUROV_12_RAM_LOCATION;
        """
        pg_hook.run(sql_statement, False)        

        # Main logic
        for loc in main_list[:3]:
            loc_name, loc_type, loc_dim = self.get_all_location_info(loc[0])  
            print(f"location info: {loc[0]}, {loc_name}, {loc_type}, {loc_dim}, {loc[1]}")
            sql_statement = f"""
            INSERT INTO v_burov_12_ram_location (id, name, type, dimension, resident_cnt)
            VALUES({loc[0]},'{loc_name}', '{loc_type}', '{loc_dim}', {loc[1]});
            """
            pg_hook.run(sql_statement, False)        

            # ON CONFLICT (id) 
            # DO NOTHING;



