import aiohttp
import asyncio
import os
os.sys.path.append("..")
from main import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from lxml import etree
from main import d


async def download_file(url, save_path):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            with open(save_path, 'wb') as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
    return save_path

async def main():
    url = 'https://vod.youlai.cn/98b3420c3f6a446eb2e7ea9ba1ffef90/4c1a68e82d7e4d4487f144007b15a031-db8d815bb014689d1eabcd8abb8b9149-sd.mp4'
    save_path = os.path.join(os.getcwd(), 'downloaded_file.mp4')
    downloaded_file = await download_file(url, save_path)
    print(f'File downloaded to: {downloaded_file}')

def fetch(url):
    result = []
    # d.delay(1)
    rsp = requests.get(url, headers=Config.headers)
    tree = etree.HTML(rsp.content)
    # ret = [tree.xpath('/html/body/div[5]/div[{}]/div/div[2]/p[1]/a/@href'.format(i))[0] for i in range(1, 21)]
    pages = tree.xpath("//*[@id='pages']/div/ul//li/a/text()")
    if len(pages) == 0:
        return result
    elif len(pages) == 1:
        return [url]
    else:    
        last_page = pages[-2]

    if last_page.isdigit():
        last_page = eval(last_page)
    else:
        #log error
        pass
    result.extend([url[:-6] + f"{page}.html" for page in range(1, last_page + 1)])
    # for suffix in ret:
    #     r = redis.Redis(connection_pool=redis_pool)
    #     r.setnx(Config.baseURL + suffix, id_2_department[url[30:-9]])
    #     result.append(Config.baseURL + suffix)
    print(result)
    print(f"获取{len(result)}个url")
    return result



def get_urls():
    with ThreadPoolExecutor(max_workers=Config.thread_num) as t:
        result = []
        task_lst = []
        # for i in range(1, 12):
            # urls = [Config.URL.format(idx + i * 10) for idx in range(1, 4)]
        urls = [Config.URL.format(i) for i in range(1, 12)]
        for url in urls:
            task = t.submit(fetch, [url])
            task_lst.append(task)

        for future in as_completed(task_lst):
            result += future.result()
    # print(result)
    return result

if __name__ == '__main__':
    # asyncio.run(main())
    #  fetch('https://www.youlai.cn/dise/imagelist/1_1.html')
    from main import Data
    data = Data()
    data.dise_name = '口腔颌面部损伤'
    data.raw_data = 'https://file.youlai.cn/cnkfile1/M00/14/58/o4YBAFlSDzaAcRJvAAC_WbEr9aQ29.jpeg'
    data.file_name = '颞下颌关节紊乱综合征 (9)'
    dirname = os.path.join(Config.dump_data_path, data.dise_name)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filepath = os.path.join(dirname, data.file_name)
    raw = requests.get(data.raw_data)
    with open(filepath, 'wb') as f:
        f.write(raw.content)