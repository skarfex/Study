
from typing import List
import json
import logging
from airflow.exceptions import AirflowException

from d_krasovskij_14_plugins.utils import trunc_json_content
# from utils import trunc_json_content


def sync_call(endpoint:str, path:str, pagination:bool, n_pages:int) -> List[dict]:
    import requests
    from requests.exceptions import RequestException

    results = []
    things_n = 0
    total_pages = 0

    def call_get(session: requests.Session, url: str, fetch_pagenum=False) -> List[dict]:
        try:
            logging.info(f'GET API: {url}')
            res = session.get(url)

            if res.status_code == 200:
                try:
                    res = json.loads(res.content)
                    if fetch_pagenum:
                        nonlocal total_pages
                        total_pages = res['info']['pages'] ### TODO: add config paths 
                        nonlocal things_n
                        things_n = res['info']['count']
                    res:list = trunc_json_content(res)
                    
                    return res

                except json.JSONDecodeError as e:
                    if fetch_pagenum:
                        raise AirflowException(e)
                        # raise Exception(e)
            else:
                raise AirflowException(f'sync_call function ERROR. response from {url} ::\n{res.content}')
                # raise Exception(f'sync_call function ERROR. response from {url} ::\n{res.content}')

        except RequestException as e:
            if fetch_pagenum:
                raise AirflowException(e)
                # raise Exception(e)

    with requests.Session() as session:
        if not pagination:
            url = f"{endpoint}/{path}"
            logging.info('without pagination')
            total_pages = 1
            results += call_get(session, url, fetch_pagenum=False)
            return results

        else:
            url_mask = lambda n: f"{endpoint}/{path}?page={n}"

            if not n_pages:
                logging.info('get pagination depth from first call')
                url = f"{endpoint}/{path}"
                total_pages = 0
                results += call_get(session, url, fetch_pagenum=True) ## mutate total_pages
                logging.info(f'total pages is: {total_pages}')
                urls:list = list(map(url_mask, range(2, total_pages+1)))

            else:
                total_pages = n_pages
                logging.info('pagination with setted pages range')
                urls:list = list(map(url_mask, range(1, total_pages+1)))

        for url in urls:
            results += call_get(session, url)

    return results
