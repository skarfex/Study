
from typing import List
from airflow.exceptions import AirflowException


def trunc_json_content(locations: dict) -> List[dict]:

    res = [{'id': loc['id'],
             'name': loc['name'],
             'type': loc['type'],
             'dimension': loc['dimension'],
             'residents_cnt': len(loc['residents'])} for loc in locations['results']]
    return res


def filter_result(lst:List[dict], n:int, by:str, reverse=False) -> List[dict]:
    if not isinstance(lst, list):
        raise AirflowException(f" 'filter_result' function accept only iterables. Given: {type(lst)}")
        # raise Exception(f" 'filter_result' function accept only iterables. Given: {type(lst)}")

    return sorted(lst,
                    reverse=reverse,
                    key=lambda x: x[by])[:n]


### some dict-utils
### TODO: write some custom recursive dict.get() for traversing

### if 'jsonpath/jmespath' libraries not allowed
# def traverse_path_dict(dct:dict) -> list:
#     lst = []

#     def traverse_dict_aux(dct:dict):
#         if not isinstance(dct, dict):
#             lst.append(dct)

#         else:
#             for k, v in dct.items():
#                 lst.append(k)
#                 traverse_dict_aux(v)

#     traverse_dict_aux(dct)
#     return lst

# a = {'info': {'pages': '1'}}
# path = traverse_path_dict(a)[:-1]

