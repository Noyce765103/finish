import requests
import sys 
sys.path.append("..")

from main import Config
from main import parse_imagelist_detail
res = requests.get("https://www.youlai.cn/dise/imagelist/950_1.html", headers=Config.headers)

parse_imagelist_detail(res.content, "https://www.youlai.cn/dise/imagelist/950_1.html")
