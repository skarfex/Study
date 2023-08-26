import json
import requests


def _get_api_data():
    # Get data from API
    r = requests.get('https://rickandmortyapi.com/api/location')

    # Convert it to python dictionary
    json_answer_text = json.loads(r.text)

    # Get necessary key
    locations = json_answer_text['results']
    return locations
locations = _get_api_data()
def _get_top_locations():

    result = []  # empty list for result
    for location in locations:
        location_res = {
            'id': location['id'],
            'name': location['name'],
            'type': location['type'],
            'dimension': location['dimension'],
            'resident_cnt': len(location['residents'])
        }
        result.append(location_res)


    # Sort list of dictionaries
    result = sorted(result, reverse=True)#[:top]
    return result
print(_get_top_locations())
print(_get_api_data())
