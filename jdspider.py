import requests
from lxml import etree
import redis
import json
import pymongo
import time
import re
'''{'goods_id:'222','title':[['xxx',shijian],...],price':[['2222','shijian'],...],'goods_info':' '}
上面这个字典是存储在mongodb的数据结构
下面这个类调用方法：
每次用的时候首先要在getid这个py文件修改获取相关商品的id
id或存入redis然后还这个Rwinfo这个类就是吧id取出然后进行访问获取具体的商品信息存入monggodb
而每次不同的商品只需要传入商品的名字参数以及对这个类进行继承重写mongdb的链接这样每个商品都可以有自己的聚集'''





class JdSpider:
    def __init__(self,name,frist_url,second_url):
        self.rdb = self.rdb = redis.Redis(host='192.168.1.60',password='******',db=3)
        self.goodsName = name
        self.frist_url = frist_url
        self.second_url = second_url
        con = pymongo.MongoClient(host='192.168.1.60',port=27017)
        jdinfo = con.JdInfo
        self.gooddb = jdinfo.goods
        self.logo = jdinfo.spider_logo
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/69.0.3497.100 Safari/537.36'}


    def get_goodsid(self):
        '''while之前是第一次请求页面为的是获取商品的页码，后面获取商品id并写入redis'''
        while True:
            reop = requests.get(url=self.frist_url + '1', headers=self.headers)
            html = etree.HTML(reop.content.decode('utf-8'))
            goods_count = int(html.xpath('//div[@id="J_topPage"]//span[@class="fp-text"]/i/text()')[0])
            page_num = goods_count*2
            for page in range(1,page_num+1,2):
                session = requests.session()
                fristreop = session.get(url=self.frist_url.replace('page=1','page={}'.format(page)),headers=self.headers)  #对url中的怕个参数进行替换达到翻页的效果
                html = etree.HTML(fristreop.content.decode('utf-8'))
                frist_goods_id = html.xpath('//li/@data-sku')
                # print(frist_goods_id)
                # print(len(frist_goods_id))
                print(fristreop.url)
                frist_data = [[i, html.xpath('//li[@data-sku="{}"]//div[@class="p-price"]//i/text()'.format(i))] for i in frist_goods_id]
                self.rdb.lpush(self.goodsName,json.dumps(frist_data))
                if len(frist_goods_id) >= 30:
                    second_headers = self.headers
                    second_headers['referer'] = fristreop.url
                    # print(second_headers)
                    second_url = self.second_url.replace('page=2','page={}'.format(page+1))+','.join(frist_goods_id)
                    second_reop = session.get(url=second_url,headers=second_headers)
                    second_html = etree.HTML(second_reop.content.decode('utf-8'))
                    second_goods_id = second_html.xpath('//li/@data-sku')
                    # print(second_goods_id)
                    print(second_reop.url)
                    second_data = [[i, second_html.xpath('//li[@data-sku="{}"]//div[@class="p-price"]//i/text()'.format(i))] for i in second_goods_id]
                    self.rdb.lpush(self.goodsName,json.dumps(second_data))
            time.sleep(180)

    # def get_price(self,idl):
    #     '''传入一个id列表你先查询返回价格和id的字典'''
    #     url = 'https://p.3.cn/prices/mgets?callback=jQuery2132725&ext=11101000&pin=&type=1&area=1_72_2799_0&skuIds=J_{}&pdbp=0&pdtk=mympuzepGPy2cZu0HpMB54HD7BkKq1TR21CyodZl53l%2BfhAyS7OyVWFzWpsi2hvX&pdpin=&pduid=682604709&so'.format('%2CJ_'.join(idl))
    #     # print(url)
    #     price_reop = requests.get(url=url,headers=self.headers)
    #     text_data = price_reop.text
    #     data_list = re.findall(r'"id":"J_(\d+)","p":"(\d+.\d{2})"',text_data)
    #     # print(data_list)
    #     return {i[0]:i[1] for i in data_list}

    def get_goods_info(self,id):
        '''这是一个处理商品信息的表达式
        [i for i in json.loads(json.dumps(aa).replace('\\n','').replace(' ','')) if i]'''
        url = 'https://item.jd.com/{}.html'.format(id)
        try:
            info_reop = requests.get(url=url, headers=self.headers)
        except Exception as a:
            error = 'id:{}发生{}'.format(id,a)
            self.logo.insert({'商品':self.goodsName,time.strftime('%y-%m-%d-%H-%M-%S',time.localtime(time.time())):error})
            print(error)
            self.rdb.lpush(self.goodsName,json.dumps([id]))
            return False,False
        else:
            try:
                reopdoc = info_reop.content.decode('gbk')
            except:
                reopdoc = info_reop.text
            info_html = etree.HTML(reopdoc)
            goods_info = info_html.xpath('//div[@class="Ptable"]//div[@class="Ptable-item"]//*/text()')
            goods_title = info_html.xpath('//head//title/text()')[0].replace('\n', '').replace(' ','')
            # print(goods_title)
            return goods_title, [i for i in json.loads(json.dumps(goods_info).replace('\\n', '').replace(' ', '')) if i]


    def write_to_mongodb(self):
        while True:
            time.sleep(0.1)
            pi = self.rdb.lpop(self.goodsName)
            if pi:
                idl = json.loads(pi)
                for data in idl:
                    p = data[1]
                    i = data[0]
                    if p:
                        p = p[0]
                    else:
                        p = '初始化'
                    # print(i)
                    title,goods_info = self.get_goods_info(id=i)  #取出商品的抬头和规格信息
                    if goods_info:
                        goods_data = self.gooddb.find_one({'goods_id': i})
                        if not goods_data:
                            self.gooddb.insert({'goods_id': i, 'title': [[title,time.time()]], 'goods_info': goods_info, 'price': [[p, time.time()]]})  #添加新商品
                            print('加入新商品：{}id：{}价格：'.format(title, i, p))
                        else:
                            if goods_data['title'][-1][0] != title:
                                self.gooddb.update({'goods_id':i},{'$push':{'title':[title,time.time()]}})
                                print('{}商品id：{}title发生变化加入数据库'.format(self.goodsName,i))
                            elif [i for i in goods_data['price'][-1]][0] != p:
                                if p != '初始化':
                                    self.gooddb.update({'goods_id':i},{'$push':{'price':[p,time.time()]}})
                                    print('{}商品di：{}price发生变化加入数据库'.format(self.goodsName,i))
