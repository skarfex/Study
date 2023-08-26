# import logging
# import requests

# 'https://rickandmortyapi.com/api/location'

# def get_dict(url):
#     """
#     Get dict responsed from API
#     :param url
#     :return dict
#     """

#     r = requests.get(url)

#     if r.status_code == 200:
#         try:
#             d = r.json()
#         except:
#             logging.warning(f"JSON load exception")
#             raise AirflowException('Error get json from response')
#     else:
#         logging.warning(f"HTTP Status is {r.status_code}")
#         raise AirflowException('Error get page')

#     return d['results']

# def top_residents_locations(locations, n):
#     """
#     Returns the id's of the first n top locations
#     :param locations, count of tops
#     :return list ids of top
#     """

#     ids = []
#     index = 0

#     for element in locations:
#         count = len(element['residents'])
#         element['count_residents']=count

#     resident_count = [[el['id'], el['count_residents']] for el in locations]
#     counts = [el['count_residents'] for el in locations]

#     counts_sorted = counts[:]
#     counts_sorted.sort(reverse=True)
#     for element in counts_sorted[:n]:
#         i = counts.index(element)
#         ids.append(resident_count[i][1])
#         counts_sorted[index] = -1
#         index+=1

#     return ids

# base_url = 'https://rickandmortyapi.com/api/location/'

# def get_locations_by_ids(base_url, ids):
#     """
#     Return locations from API by given id's
#     :param api url, id's
#     :return list locations by id's
#     """

#     locations = []
#     for id in ids:
#         location = get_dict(base_url + id)
#         locations.append(location)

#     return locations
