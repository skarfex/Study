import requests
import json


response = requests.get('https://rickandmortyapi.com/api/location')
data = json.loads(response.text)
results = data.get('results')

for i in range(len(results)):
    results[i]['resident_cnt'] = len(results[i].get('residents', []))

results.sort(key=lambda x: x['resident_cnt'], reverse=True)


transformed_data = []
for el in results[:3]:
    transformed_data.append({
        'id': el['id'],
        'name': el['id'],
        'type': el['type'],
        'dimension': el['dimension'],
        'resident_cnt': el['resident_cnt']
    })

values = []
for el in transformed_data:
    values.append(el.values())

print(values)