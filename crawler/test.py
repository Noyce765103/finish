import requests
from main import parse_youlai, Config
from lxml import etree

TEST_URL = "https://www.youlai.cn/dise/1134.html"
rsp = requests.get(TEST_URL, headers=Config.headers)
parse_youlai(rsp.content)