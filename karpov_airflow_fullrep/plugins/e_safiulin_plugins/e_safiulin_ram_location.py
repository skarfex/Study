import logging
import requests

from typing import List
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.http.hooks.http import HttpHook



class EvgeniiSafiulinRickMortyTool:
	"""
	Web hook for R&M API interaction
	"""
#	api_url = 'https://rickandmortyapi.com/api'

	def __init__(self, base_url:str) -> None:
		self.api_url = base_url

	def _api_call(self, path:str, params:dict = {}):
		req = requests.get(self.api_url + path, params=params)
		if req.status_code == 200:
			logging.info('Get pages request status: 200')
			return req
		else:
			logging.warning("HTTP STATUS {}".format(req.status_code))
			raise AirflowException('Error in load page count')

	def get_page_count(self, path:str = '/location') -> int:
		response = self._api_call(path)
		page_amount = response.json().get('info').get('pages')
		return page_amount

	def get_locations_by_page(self, page_number:int, path:str = '/location'):
		response = self._api_call(path, params={'page':page_number})
		return response.json()['results']

class EvgeniiSafiulinRamLocationOperator(BaseOperator):
	"""
	Find top locations in R&M and push them to db
	"""
	ui_color = "#e0ffff"

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
	
	def get_location_data(self, results_from_hook:list) -> List[dict]:
		"""
		Get locations from api and convert it to a list for db

		:param json_from_page
			response list from page request
		:return List[dict]
			list with dicts : [{'id':..., 'name':..., 'type'..., 'dimenstion':..., 'residents_cnt':...'}] 
		"""
		locations = [{'id':locat['id'],
					'name': locat['name'],
					'type':locat['type'],
					'dimension':locat['dimension'],
					'resident_cnt':len(locat['residents'])} for locat in results_from_hook]
		return locations
		
	def data_base_init(self, sql_hook, tab_name:str):
		"""
		Put data into GP table tab_name

		:param sql_hook
			postgres hook for connection to GP
		:param tab_name
			string name of destination table
		"""
		sql_hook.run(f"""
		CREATE TABLE IF NOT EXISTS {tab_name}
		(
			id INT PRIMARY KEY,
			name TEXT NULL,
			type TEXT NULL,
			dimension TEXT NULL,
			resident_cnt INT NULL
		)
		DISTRIBUTED BY (id);
			
		DELETE FROM {tab_name};
		""", False)
	
	def get_top_residents_locations(self, locations_data:list, top_number:int):
		"""
		Returns top_number values sorted by count of location residents

		:param int
			The number of top values i.e: 3 -> is going to top 3
		:return list
			A list with length top_number values sorted by count of residents
		"""
		return sorted(locations_data, key=lambda x: x['resident_cnt'], reverse=True)[:top_number]

	def execute(self, context):
		"""
		Logging requested pages and put in the GreenPlum
		"""
		tab_name = 'e_safiulin_ram_location'
		http_hook = EvgeniiSafiulinRickMortyTool('https://rickandmortyapi.com/api')
		pg_hook = PostgresHook('conn_greenplum_write')
		# Create if not table, clear tablle before insert
		self.data_base_init(pg_hook, tab_name)
		logging.info(f'Table {tab_name} created and cleanned')
		# Get top 3 from all pages
		locations_data = []
		for page in range(1, http_hook.get_page_count() + 1):
			logging.info(f'Running page {page}')
			locations_data += self.get_location_data(http_hook.get_locations_by_page(page))
		logging.info('Location data was filled')
		top_locations = self.get_top_residents_locations(locations_data, 3)
		logging.info(f'The top locations by residents are: {[loc["name"] for loc in top_locations]}')
		# Put them into destination table
		to_insert_values = ', '.join([f"{elem['id'], elem['name'], elem['type'], elem['dimension'], elem['resident_cnt']}" for elem in top_locations])
		pg_hook.run(f"INSERT INTO {tab_name} VALUES {to_insert_values}", False)
		logging.info(f'Values was pushed to GP {tab_name}')
