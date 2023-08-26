import requests
import logging

from airflow.models import BaseOperator
import pandas as pd

logging.basicConfig(level=logging.INFO)


class RickandmortyOperator(BaseOperator):

    url = 'https://rickandmortyapi.com/api'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_json(url):
        resp = requests.get(url=url)
        if resp.status_code == 200:
            logging.debug('Success')
            return resp.json()
        else:
            logging.warning(f'ERROR: {resp.status_code}')
            raise Exception()

    def _get_all_pages(self, url_suffix):
        full_list = []
        resp = RickandmortyOperator._get_json(RickandmortyOperator.url + '/' + url_suffix)

        cnt = 1
        while True:
            logging.info('Processing page {0}/{1}'.format(cnt, resp['info']['pages']))

            full_list = full_list + resp['results']

            if resp['info']['next'] is not None:
                resp = RickandmortyOperator._get_json(resp['info']['next'])
                cnt += 1
            else:
                break

        return full_list


class RickandmortyTopLocsOperator(RickandmortyOperator):

    def __init__(self, top_locations, **kwargs):
        super().__init__(**kwargs)
        self.top_locations = top_locations

    # def get_locations_by_chars(self):
    #     chars = pd.DataFrame(self._get_all_pages('character'))
    #
    #     for col in ['origin', 'location']:
    #         chars[col] = chars[col].map(lambda x: x['name'])
    #
    #     locs_grp = chars[['id','location']].groupby(by='location', as_index=False).count().rename({'id':'cnt'}, axis=1)
    #     locs_grp.sort_values(by='cnt', ascending=False, inplace=True)
    #
    #     return locs_grp

    def get_locations(self):
        locs = pd.DataFrame(self._get_all_pages('location'))
        locs['resident_cnt'] = locs['residents'].apply(lambda x: len(x))
        return locs[['id','name','type','dimension','resident_cnt']]

    def execute(self, context, **kwargs):
        locs = self.get_locations()
        locs.sort_values(by='resident_cnt', ascending=False, inplace=True)
        locs = locs.head(self.top_locations)
        return locs