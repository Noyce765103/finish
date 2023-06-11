import aiohttp
import asyncio
import requests
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis
import re
import pymysql
import os
from queue import Queue
from threading import Thread
from aiohttp.client_exceptions import ClientConnectionError, ServerDisconnectedError, ServerTimeoutError
from lxml import etree

from common.delay import Delay

import logging
import traceback
 
logger = logging.getLogger('crawler_logger')
logger.setLevel(logging.DEBUG)
test_log = logging.FileHandler('crawler.log','a',encoding='utf-8')
test_log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(filename)s - line:%(lineno)d - %(levelname)s - %(message)s - %(process)s')
test_log.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(test_log)
logger.addHandler(ch)

ua = UserAgent()


class Config:
    thread_num = 12
    # URL = "https://www.miaoshou.net/question/list_4_0_{}.html"
    URL = "https://www.youlai.cn/dise/pk_{}_0_1.html"
    # baseURL = "https://www.miaoshou.net"
    baseURL = "https://www.youlai.cn"
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
    dump_data_path = ''
    redis_addr = '127.0.0.1'
    mysql_addr = '127.0.0.1'
    mysql_port = 3306
    mysql_user = 'root'
    mysql_password = ''
    mysql_db = 'medical_kg'
    


class Context:
    def __init__(self) -> None:
        # self.startUrl = StartUrl
        self.nextUrl = None
        self.databus = Queue()
        self.terminate = False

class Data:
    def __init__(self) -> None:
        self.target = None          # 'file' or 'mysql'
        self.raw_data = None        # raw data
        self.file_name = None
        self.table = None
        self.dise_name = None


# resource instance
d = Delay(ub=Config.delay['upper_bound'])
redis_pool = redis.ConnectionPool(host=Config.redis_addr)
db = pymysql.connect(host=Config.mysql_addr,
                     user=Config.mysql_user,
                     port=Config.mysql_port,
                     passwd=Config.mysql_password,
                     database=Config.mysql_db)

id_2_department = {
    "1":"内科",
    "2":"外科",
    "3":"妇产科",
    "4":"儿科",
    "5":"男科",
    "6":"皮肤性病科",
    "7":"五官科",
    "8":"肿瘤科",
    "9":"精神心理科",
    "10":"不孕不育",
    "11":"其他",
}


def fetch(urls):
    result = []
    for url in urls:
        d.delay(1)
        rsp = requests.get(url, headers=Config.headers)
        tree = etree.HTML(rsp.content)
        # ret = [tree.xpath('/html/body/div[5]/div[{}]/div/div[2]/p[1]/a/@href'.format(i))[0] for i in range(1, 21)]
        ret = tree.xpath('/html/body/div[2]/div/div[2]/div[2]/div/dl/dt/a/@href')
        for suffix in ret:
            r = redis.Redis(connection_pool=redis_pool)
            r.setnx(Config.baseURL + suffix, id_2_department[url[30:-9]])
            result.append(Config.baseURL + suffix)
    print(f"获取{len(result)}个url")
    return result

def fetch_imageListUrl(url):
    r = redis.Redis(connection_pool=redis_pool)
    if r.hexists("imagelist_urls", url):
        logger.info(f"已爬取过[{url}],跳过...")
        imgurl = r.hget("imagelist_urls", url)
        if(imgurl.decode('utf-8') == ''):
            return []
        return imgurl.decode('utf-8').split(',')
    d.sleep(2)
    logger.info(f"开始获取url[{url}]...")
    result = []
    rsp = requests.get(url, headers=Config.headers)
    tree = etree.HTML(rsp.content)
    pages = tree.xpath("//*[@id='pages']/div/ul//li/a/text()")
    if len(pages) == 0:
        r.hsetnx("imagelist_urls", url, '')
        return result
    elif len(pages) == 1:
        r.hsetnx("imagelist_urls", url, url)
        return [url]
    else:    
        last_page = pages[-2]

    if last_page.isdigit():
        last_page = eval(last_page)
    else:
        #log error
        logger.error(f"解析页数失败，url:[{url}]")
    result += [url[:-6] + f"{page}.html" for page in range(1, last_page + 1)]
    
    r.hsetnx("imagelist_urls", url, ','.join(result))
    logger.info(f"获取{len(result)}个url")
    return result


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


def parse_miaoshou(Content):
    tree = etree.HTML(Content)
    question = tree.xpath('/html/body/div[4]/div[2]/div[1]/div[1]/div[1]/h1/span/text()')[0]
    answer = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[1]/div/text()')[0]
    suggestion = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[2]/div/text()')[0]
    return [question, answer, suggestion]


def parse_youlai_detail(Content, url):
    try:
        tree = etree.HTML(Content)
        dise_id = url[27:-5]
        name = tree.xpath('/html/body/div[2]/div/p/text()')[0].strip()
        alias_name = tree.xpath('/html/body/div[2]/div/p/span/text()')
        if len(alias_name) == 0:
            alias_name = ''
        else:
            alias_name = alias_name[0]
            idx = alias_name.find('：')
            if idx != -1:
                alias_name = alias_name[idx+1:-1].strip()
        departments = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[1]/span/a/text()')[0].strip()
        related_symptom = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[1]/span/text()')
        if len(related_symptom) == 0:
            related_symptom = ''
        else:
            related_symptom = related_symptom[0].strip()
        buwei = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[2]/span/text()')
        if len(buwei) == 0:
            buwei = ''
        else:
            buwei = buwei[0].strip()
        related_diseases = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[2]/span/a/text()')[0].strip()
        affected_people = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[3]/span/text()')[0].strip()
        related_inspect = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[3]/span/text()')[0].strip()
        treatment = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[5]/span/text()')[0].strip()
        related_operation = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[4]/span/text()')[0].strip()
        is_infect = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[5]/span/text()')[0].strip()
        related_drugs = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[5]/span/text()')[0].strip()
        is_inherit = tree.xpath('/html/body/div[3]/div[1]/dl/dt/p[6]/span/text()')[0].strip()
        treat_cost = tree.xpath('/html/body/div[3]/div[1]/dl/dd[1]/p[6]/span/text()')[0].strip()
        pattern = r"\d+-\d+"  
        result = re.search(pattern, treat_cost)  
        if result:  
            treat_cost = result.group()
        else:  
            treat_cost = ""
        r = redis.Redis(connection_pool=redis_pool)
        clinic = "" if r.get(url) is None else r.get(url).decode('utf-8')
        d = Data()
        d.raw_data = [dise_id, name, alias_name, clinic, departments, related_symptom, buwei, related_diseases, affected_people, 
                related_inspect, treatment, related_operation, is_infect, related_drugs, is_inherit, treat_cost]
        d.target = 'mysql'
        d.table = 'disease'

        ctx.databus.put(d)
        return True
    except Exception as e:
        logger.error(f"爬取{url}失败！错误原因: {traceback.format_exc()}")
        return False

def parse_imagelist_detail(Content, url):
    try:
        tree = etree.HTML(Content)
        disease_name = tree.xpath('/html/body/div[2]/div/p/text()')[0].strip()
        file_urls = tree.xpath('/html/body/div[3]/div[1]/a/div/img/@src')
        file_names = tree.xpath('/html/body/div[3]/div[1]/a/p/text()')
        for i in range(0, len(file_urls)):    
            data = Data()
            data.target = 'file'
            data.raw_data = "https:"+file_urls[i]
            data.file_name = file_names[i]
            data.dise_name = disease_name
            ctx.databus.put(data)
        return True

    except Exception as e:
        logger.error(f"爬取{url}失败！错误原因: {traceback.format_exc()}")
        return False

total = 0
remain = 0
failed = 0
success = 0
skip = 0

async def get_and_parse_page(url, parse_func, semaphore):
    global success, failed, success, skip
    connector = aiohttp.TCPConnector(limit=10, force_close=True)
    r = redis.Redis(connection_pool=redis_pool)
    if type(url) is not list:
        url = [url]
    for u in url:
        if r.sismember("success_urls", u):
            skip += 1
            logger.info(f"已爬取过该页面[{u}],跳过爬取...")
            continue
        tries = 0
        while tries < 5:
            try:
                async with semaphore:
                    async with aiohttp.ClientSession(headers=Config.headers, connector=connector) as session:
                        async with session.get(u) as rsp:
                            d.delay(1)
                            print("crawling====>", url)
                            content = await rsp.read()
                            res = parse_func(content, u)
                            
                            if res:
                                success += 1
                                r.sadd("success_urls", u)
                            else:
                                failed += 1
                                r.sadd('failed_url', u)
                            logger.info(f"成功爬取: {success}个, 失败爬取: {failed}个, 跳过爬取: {skip}个, 当前进度: {success + failed + skip}/{total}")
                            return res
            except ServerDisconnectedError or ServerTimeoutError:
                logger.warn(f"服务端超时异常, url[{u}], 重试第{tries}次...")
                tries += 1
            except ClientConnectionError as e:
                logger.warn(f"客户端超时异常, url[{u}], 重试第{tries}次...")
                tries += 1
        logger.error(f"爬取url[{u}]失败, 重试次数:{tries}")
        r.sadd('failed_url', u)
        logger.info(f"成功爬取: {success}个, 失败爬取: {failed}个, 跳过爬取: {skip}个, 当前进度: {success + failed + skip}/{total}")



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

def get_urls_imagelist():
    cursor = db.cursor()
    cursor.execute('''select id from disease''')
    raw = cursor.fetchall()
    with ThreadPoolExecutor(max_workers=Config.thread_num) as t:
        result = []
        task_lst = []
        # for i in range(1, 12):
            # urls = [Config.URL.format(idx + i * 10) for idx in range(1, 4)]
        urls = [f"https://www.youlai.cn/dise/imagelist/{i[0]}_1.html" for i in raw]
        for url in urls:
            task = t.submit(fetch_imageListUrl, url)
            task_lst.append(task)
        logger.info(f"共计{len(task_lst)}个task, 开始获取url")

        for future in as_completed(task_lst):
            result += future.result()
    # print(result)
    return result


def dumpData(data):
    with open(Config.dump_data_path, 'w', encoding="utf-8") as f:
        for line in data:
            f.write('|'.join(line) + '\n')


def dumpFile():
    dump_cnt = 0
    failed_dump = 0
    r = redis.Redis(connection_pool=redis_pool)
    try:
        while not ctx.terminate or not ctx.databus.empty():
            if ctx.databus.empty():
                logger.info("暂时没有下载任务...")
                d.delay(1)
                continue
            data = ctx.databus.get()
            try:
                dirname = os.path.join(Config.dump_data_path, data.dise_name)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                filetype = data.raw_data[data.raw_data.rfind('.'):]
                filepath = os.path.join(dirname, data.file_name + filetype)
                raw = requests.get(data.raw_data)
                if os.path.exists(filepath):
                    logger.info(f"文件已存在:[{filepath}] ...")
                else:
                    with open(filepath, 'wb') as f:
                        f.write(raw.content)
                    r.sadd("success_download_file",filepath)
                    r.sadd("success_download_url", data.raw_data)
                    logger.info(f"成功下载文件, 文件路径[{filepath}], url[{data.raw_data}]")
                    dump_cnt += 1
            except Exception as e:
                r.sadd("failed_download_file",filepath)
                r.sadd("failed_download_url", data.raw_data)
                logger.error(f"下载文件失败, 文件路径[{filepath}], url[{data.raw_data}]")
                logger.error(f"{traceback.format_exc()}")
                failed_dump += 1
            finally:
                logger.info(f"成功下载: {dump_cnt}个, 失败下载: {failed_dump}个")
                d.sleep(1)
    except Exception as e:
        logger.error(f"下载文件出现异常，总共下载{dump_cnt}张图片！")
        logger.error(f"{traceback.format_exc()}")


def dumpMysql():
    buffersize = 16
    buffer = {}
    cursor = None
    dump_cnt = 0
    try:
        while not ctx.terminate or not ctx.databus.empty():
            for key in buffer.keys():
                if len(buffer[key]) >= buffersize:
                    # 写数据库表
                    data_buff = buffer[key]
                    table_name = key
                    data_str = ','.join(map(lambda x: f"({x})", [','.join(map(lambda x: f"'{x}'", data.raw_data)) for data in data_buff]))
                    sql = f'''insert into {table_name} values {data_str}'''
                    if cursor is None:
                        cursor= db.cursor()
                    cursor.execute(sql)
                    db.commit()
                    # TODO:log
                    dump_cnt += len(data_buff)
                    logger.info(f"成功向数据库写入{dump_cnt}条记录!")
                    buffer[key].clear()
            if cursor is not None:
                cursor.close()
            cursor = None
                
            if not ctx.databus.empty():
                data = ctx.databus.get()
                if buffer.get(data.table, []) == []:
                    buffer[data.table] = [data]
                else:
                    buffer[data.table].append(data)
            d.sleep(1)
            
        for key in buffer.keys():
            if len(buffer[key]) > 0:
                # 写数据库表
                data_buff = buffer[key]
                table_name = key
                data_str = ','.join(map(lambda x: f"({x})", [','.join(map(lambda x: f"'{x}'", data.raw_data)) for data in data_buff]))
                sql = f'''insert into {table_name} values {data_str}'''
                if cursor is None:
                    cursor= db.cursor()
                cursor.execute(sql)
                db.commit()
                # TODO:log
                dump_cnt += len(data_buff)
                logger.info(f"成功向数据库写入{dump_cnt}条记录! sql:[{data_str}]")
                buffer[key].clear()
        if cursor is not None:
            cursor.close()
    except Exception as e:
        if cursor is not None:
            cursor.close()
        logger.error(f"写数据库出现异常，总共写入{dump_cnt}条记录！")
        logger.error(f"{traceback.format_exc()}")

    


def DiseaseJob(startUrl):
    ctx = Context()
    dump_thread = Thread(target=dumpMysql, args=(ctx,))
    dump_thread.start()
    get_and_parse_page(url, parse_youlai_detail)
    get_and_parse_page(url, parse_youlai_videolist)
    get_and_parse_page(url, parse_youlai_video_detail)
    get_and_parse_page(url, parse_youlai_articlelist)
    get_and_parse_page(url, parse_youlai_article_detail)
    get_and_parse_page(url, parse_youlai_asklist)
    get_and_parse_page(url, parse_youlai_asklist_detail)
    ctx.terminate = True
    dump_thread.join()


ctx = Context()
# URL = "https://www.miaoshou.net/question/YqxrB378eQmge6R8.html"
def main():
    global total
    try:
        print("开始获取url...")
        # url_list = get_urls()
        # r = redis.Redis(connection_pool=redis_pool)
        # success_urls = r.smembers("success_urls")
        # failed_url = r.smembers("failed_url")
        # success_urls = set(map(lambda x: x.decode('utf-8'), success_urls))
        # failed_url = set(map(lambda x: x.decode('utf-8'), failed_url))
        # cursor = db.cursor()
        # cursor.execute('''select id from disease''')
        # raw = cursor.fetchall()
        # mysql_record_num = len(raw)
        # print(f"数据库记录条数{mysql_record_num}")
        # mysql_list = set([f"https://www.youlai.cn/dise/{i[0]}.html" for i in raw])
        # url_list = set([f"https://www.youlai.cn/dise/{i}.html" for i in range(1, 2231)])
        # url_list = url_list  - mysql_list
        # dump_thread = Thread(target=dumpMysql)
        # dump_thread.start()
        url_list = get_urls_imagelist()
        dump_thread = Thread(target=dumpFile)
        dump_thread.start()
        semaphore = asyncio.Semaphore(10)
        total = len(url_list)
        print(f"总共获取{total}个url, 开始爬取解析...")
        # tasks = [asyncio.ensure_future(get_and_parse_page(url, parse_youlai_detail, semaphore)) for url in url_list]
        tasks = [asyncio.ensure_future(get_and_parse_page(url, parse_imagelist_detail, semaphore)) for url in url_list]
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*tasks)
        res = loop.run_until_complete(tasks)
    # dumpData(res)
    except Exception as e:
        logger.error(f"爬取流程出现错误:{traceback.format_exc()}...")
    finally:
        ctx.terminate = True
    dump_thread.join()


def check():
    cursor = db.cursor()
    cursor.execute('''select id from disease''')
    raw = cursor.fetchall()
    mysql_record_num = len(raw)
    r = redis.Redis(connection_pool=redis_pool)
    redis_record_num = r.scard("success_urls")
    if mysql_record_num == redis_record_num:
        logger.info("Mysql记录与Redis记录数量一致, 检测完成...")
    elif mysql_record_num > redis_record_num:
        mysql_urls = [f"https://www.youlai.cn/dise/{d[0]}.html" for d in raw]
        r.sadd("success_urls", *mysql_urls)
        r.srem("failed_url", *mysql_urls)
        logger.info(f"Mysql同步{len(raw)}条记录至Redis, 同步完成...")
    else:
        mysql_urls = [f"https://www.youlai.cn/dise/{d[0]}.html" for d in raw]
        r.srem("success_urls", *mysql_urls)
        r.srem("failed_url", *mysql_urls)
        logger.info(f"Mysql同步{len(raw)}条记录至Redis, 同步完成...")


if __name__ == '__main__':
    # cursor = db.cursor()
    # cursor.execute('''select id from disease''')
    # raw = cursor.fetchall()
    # print(raw)
    # r = redis.Redis(connection_pool=redis_pool)
    # r.sadd("success_urls", [f"https://www.youlai.cn/dise/{r[0]}.html" for r in raw])
    # check()
    main()
    db.close()
