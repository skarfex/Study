"""
Location from API https://rickandmortyapi.com/api/location
С помощью API (https://rickandmortyapi.com/documentation/#location)
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

Algorithm:
    1.Get a list of locations (json)
    2.Calculate number of resident for each location
    3.Sort table by cnt_residents.
    4.Write into table top 3 values.
"""

import requests
import pandas as pd

locations = pd.DataFrame(columns=['id', 'name', 'type', 'dimension', 'resident_cnt'])

for page in range(1,get_loc_page_count(url.format(pg=1))+1):
    response = requests.get(url.format(pg=page))
    if response.status_code == 200:
        # Access the JSON data from the response
        data = response.json()
        results = data['results']

        for result in results:
            print(result)
            row  = {
                           'id': result['id'],
                         'name': result['name'],
                         'type': result['type'],
                   'dimension' : result['dimension'],
               'resident_cnt': len(result['residents'])
            }
            locations.loc[len(locations)]  = row
        print(locations)
        print(get_loc_page_count(url.format(pg=page)))
    else:
        # Request was not successful
        print("Error:", response.status_code)