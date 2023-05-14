import asyncio
import requests
import aiohttp

for i in range(0, 5):
    urls = ["https://www.miaoshou.net/question/list_4_0_{}.html".format(idx + i * 10) for idx in range(1, 11)]
    print(urls)