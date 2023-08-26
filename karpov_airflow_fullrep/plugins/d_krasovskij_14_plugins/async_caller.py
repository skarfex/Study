
import sys
import asyncio
import aiohttp
import logging
import json
from typing import List

from d_krasovskij_14_plugins.utils import trunc_json_content


def async_call(endpoint:str, path:str, pagination:bool, n_pages:int) -> List[dict]:

    async def async_template_get(session,
                                 url,
                                 result_to: list, 
                                 status_dict: dict,
                                 fetch_pagenums=False
                                ):
        if not fetch_pagenums:
            await event.wait()
        try:
            async with session.get(url) as resp:
                logging.info(f"collecting: {url}")
                if resp.status == 200:
                    res = await resp.text()
                    res = json.loads(res)
                    if fetch_pagenums:
                        status_dict['total_pages'] = res['info']['pages']
                        status_dict['need'] = res['info']['count']
                        event.set()
                    res = trunc_json_content(res)
        except Exception as e:
            logging.error(e)
            res = [None]
        # nonlocal results
        # results += res
        result_to += res


    async def async_main(result_to, status_dict):

        async with aiohttp.ClientSession() as session:
            if not pagination:
                url = f"{endpoint}/{path}"
                status_dict['total_pages'] = 1
                await async_template_get(session=session,
                                        url=url,
										result_to=result_to,
										fetch_pagenums=False,
										status_dict=status_dict
										)

            else:
                url_mask = lambda n: f"{endpoint}/{path}?page={n}"
                if not n_pages:
                    await async_template_get(session=session,
                                            url=url_mask(1),
                                            result_to=result_to,
                                            fetch_pagenums=True,
                                            status_dict=status_dict
                                            )
                    urls:list = list(map(url_mask, range(2, status_dict['total_pages']+1)))

                else:
                    urls:list = list(map(url_mask, range(1, status_dict['total_pages']+1)))

            for url in urls:
                await async_template_get(session=session,
                                        url=url,
                                        result_to=result_to,
                                        fetch_pagenums=False,
                                        status_dict=status_dict
                                        )

    def collect_in_loop():

        status_dict = {'need': 0, 'done': 0, 'total_pages': 0}

        try:
            results = []
            if sys.platform == 'win32':
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
            asyncio.run(async_main(result_to=results, status_dict=status_dict))

            ### TODO: write logs
            # status_dict['done'] = len([i for i in res_jsons if i])
            # logging.info(f"Collected: {status_dict['done']}. Missed: {status_dict['need'] - status_dict['done']}")
        except Exception as e:
            logging.error(e)
            
        return results

    event = asyncio.Event()
    result = collect_in_loop()
    
    return result