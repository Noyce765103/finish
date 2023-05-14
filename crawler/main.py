import requests
from fake_useragent import UserAgent
from lxml import etree

ua = UserAgent()
headers = {
    'User-Agent':ua.random
}
URL = "https://www.miaoshou.net/question/list_4_0_1_1.html"
# URL = "https://www.miaoshou.net/question/YqxrB378eQmge6R8.html"
def main():
    rsp = requests.get(URL, headers=headers)
    tree = etree.HTML(rsp.content)
    ret = tree.xpath('/html/body/div[5]/div[1]/div/div[2]/p[1]/a/@href')[0]
    rsp = requests.get(f'https://www.miaoshou.net{ret}', headers=headers)
    tree = etree.HTML(rsp.content)
    question = tree.xpath('/html/body/div[4]/div[2]/div[1]/div[1]/div[1]/h1/span/text()')
    answer = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[1]/div/text()')[0]
    suggestion = tree.xpath('//*[@id="main"]/div[2]/div[2]/div[2]/div[2]/div/text()')[0]
    print(f'''
        问题：{question}
        回答：{answer}
        建议：{suggestion}
    ''')


if __name__ == '__main__':
    main()