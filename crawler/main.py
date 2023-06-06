import aiohttp
import asyncio
import requests
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor, as_completed

from lxml import etree

from common.delay import Delay

ua = UserAgent()


class Config:
    thread_num = 1
    URL = "https://www.miaoshou.net/question/list_4_0_{}.html"
    baseURL = "https://www.miaoshou.net"
    headers = {
        'User-Agent': ua.random,
        'Referer': 'https://www.youlai.cn/dise/pk_4_0_1.html'
    }
    delay = {
        'upper_bound': 4,
        'lower_bound': 1,
        'miu': 0,
        'sigma': 0.5
    }
    dump_data_path = '../../miaoshou.txt'


d = Delay(ub=Config.delay['upper_bound'])


def fetch(urls):
    result = []
    for url in urls:
        d.delay(3)
        rsp = requests.get(url, headers=Config.headers)
        tree = etree.HTML(rsp.content)
        ret = [tree.xpath('/html/body/div[5]/div[{}]/div/div[2]/p[1]/a/@href'.format(i))[0] for i in range(1, 21)]
        result += ret
    return map(lambda suffix: Config.baseURL + suffix, result)


def parse_miaoshou(Content):
    tree = etree.HTML(Content)
    question = tree.xpath('/html/body/div[4]/div[2]/div[1]/div[1]/div[1]/h1/span/text()')[0]
    answer = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[1]/div/text()')[0]
    suggestion = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[2]/div/text()')[0]
    return [question, answer, suggestion]


def parse_youlai(Content):
    tree = etree.HTML(Content)
    departments = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[1]/span/a/text()')
    related_symptom = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[1]/span/text()')
    buwei = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[2]/span/text()')
    related_diseases = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[2]/span/a/text()')
    affected_people = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[3]/span/text()')
    related_inspect = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[3]/span/text()')
    treatment = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[5]/span/text()')
    related_operation = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[4]/span/text()')
    is_infect = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[5]/span/text()')
    related_drugs = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[5]/span/text()')
    is_inherit = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[6]/span/text()')
    treat_cost = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[6]/span/text()')
    print(f'''
        departments:\t{departments}
        related_symptom:\t{related_symptom}
        buwei:\t{buwei}
        related_diseases:\t{related_diseases}
        affected_people:\t{affected_people}
        related_inspect:\t{related_inspect}
        treatment:\t{treatment}
        related_operation:\t{related_operation}
        is_infect:\t{is_infect}
        related_drugs:\t{related_drugs}
        is_inherit:\t{is_inherit}
        treat_cost:\t{treat_cost}
    ''')




async def get_and_parse_page(url, parse_func):
    async with aiohttp.ClientSession(headers=Config.headers) as session:
        async with session.get(url) as rsp:
            print("crawling====>", url)
            content = await rsp.read()
            return parse_func(content)


def get_urls():
    with ThreadPoolExecutor(max_workers=Config.thread_num) as t:
        result = []
        task_lst = []
        for i in range(0, 1):
            urls = [Config.URL.format(idx + i * 10) for idx in range(1, 4)]
            task = t.submit(fetch, urls)
            task_lst.append(task)

        for future in as_completed(task_lst):
            result += future.result()
    print(result)
    return result


def dumpData(data):
    with open(Config.dump_data_path, 'w', encoding="utf-8") as f:
        for line in data:
            f.write('|'.join(line) + '\n')

# URL = "https://www.miaoshou.net/question/YqxrB378eQmge6R8.html"
def main():
    url_list = get_urls()
    tasks = [asyncio.ensure_future(get_and_parse_page(url, parse_miaoshou)) for url in url_list[:3]]
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*tasks)
    res = loop.run_until_complete(tasks)
    dumpData(res)


if __name__ == '__main__':
    main()
