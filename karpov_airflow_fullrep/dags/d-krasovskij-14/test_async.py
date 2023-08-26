import sys
import time
import asyncio
import aiohttp
import json
from typing import List


event = asyncio.Event()

ram_api_loc_page = lambda n: f'https://rickandmortyapi.com/api/location?page={n}'


def trunc_json(locs: dict) -> List[dict]:

    res = [{'id': loc['id'],
             'name': loc['name'],
             'type': loc['type'],
             'dimension': loc['dimension'],
             'residents_cnt': len(loc['residents'])} for loc in locs['results']]

    return res

async def async_template_get(session,
                            page,
                            append_to, 
                            status_dict,
                            fetch_pagenums=False
                            ):
    if not fetch_pagenums:
        await event.wait()
    try:
        url = ram_api_loc_page(page)
        async with session.get(url) as resp:
            print(url)
            if resp.status == 200:
                res = await resp.text()
                res = json.loads(res)
                if fetch_pagenums:
                    status_dict['total_pages'] = res['info']['pages']
                    status_dict['need'] = res['info']['count']
                    event.set()
                res = trunc_json(res)
    except Exception as e:
        print(e)
        res = [None]
    append_to += res


async def async_main(append_to, status_dict):
    async with aiohttp.ClientSession() as session:
        await async_template_get(session=session,
                                page=1,
                                append_to=append_to,
                                fetch_pagenums=True,
                                status_dict=status_dict
                                )
        
        for page in range(2, status_dict['total_pages']+1):
            await async_template_get(session=session,
                                    page=page,
                                    append_to=append_to,
                                    fetch_pagenums=False,
                                    status_dict=status_dict
                                    )


def collect_in_loop():
    status_dict = {'need': 0,
                    'done': 0,
                    'total_pages': 0}
    try:
        res_jsons = []
        print(f"nums before: {status_dict['total_pages']}")
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(async_main(append_to=res_jsons, status_dict=status_dict))

        print(f"\nnums after: {status_dict['total_pages']}")
        status_dict['done'] = len([i for i in res_jsons if i])
        print(f"Collected: {status_dict['done']}. Missed: {status_dict['need'] - status_dict['done']}")
    except Exception as e:
        print(e)
        
    return res_jsons


time_from = time.time()
result = collect_in_loop()
        
top_three_populated = sorted(result,
                            reverse=True,
                            key=lambda x: x['residents_cnt'])[:3]

time_to = time.time()
print(f"time: {time_to - time_from}")
print(top_three_populated)
