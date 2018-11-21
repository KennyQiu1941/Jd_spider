from jdspider import JdSpider
import pymongo
import threading
'''这里是从数据库获取商品id并进行爬取商品信息存入mongodb
为了每个类别可以存放在不同的聚合里每个类别需要改写构造函数来重新定义mangodb的链接'''


class Shouji(JdSpider):
    def __init__(self,name,frist_url,second_url):
        super(Shouji,self).__init__(name,frist_url,second_url)
        con = pymongo.MongoClient(host='192.168.1.60', port=27017)
        jdinfo = con.JdInfo
        self.gooddb = jdinfo.shoujiinfo


class Kongtiao(JdSpider):
    def __init__(self,name,frist_url,second_url):
        super(Kongtiao,self).__init__(name,frist_url,second_url)
        con = pymongo.MongoClient(host='192.168.1.60', port=27017)
        jdinfo = con.JdInfo
        self.gooddb = jdinfo.kongtiao
        
    
class Cpu(JdSpider):
    def __init__(self,name,frist_url,second_url):
        super(Cpu,self).__init__(name,frist_url,second_url)
        con = pymongo.MongoClient(host='192.168.1.60', port=27017)
        jdinfo = con.JdInfo
        self.gooddb = jdinfo.cpuinfo
        

class Disk(JdSpider):
    def __init__(self,name,frist_url,second_url):
        super(Disk,self).__init__(name,frist_url,second_url)
        con = pymongo.MongoClient(host='192.168.1.60', port=27017)
        jdinfo = con.JdInfo
        self.gooddb = jdinfo.diskinfo


shoujispider = Shouji(name='手机',frist_url='https://search.jd.com/Search?keyword=%E6%89%8B%E6%9C%BA&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=shou%27ji&cid2=653&cid3=655&wtype=1&page=1',second_url='https://search.jd.com/s_new.php?keyword=%E6%89%8B%E6%9C%BA&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=shou%27ji&cid2=653&cid3=655&wtype=1&page=2&s=30&scrolling=y&log_id=1540218596.67404&tpl=3_M&show_items=')
sjgetid = shoujispider.get_goodsid
sjwrite = shoujispider.write_to_mongodb

kongtiaospider = Kongtiao('空调','https://search.jd.com/search?keyword=%E7%A9%BA%E8%B0%83&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E7%A9%BA%E8%B0%83&cid2=794&cid3=870&stock=1&wtype=1&page=1&s=1&click=0','https://search.jd.com/s_new.php?keyword=%E7%A9%BA%E8%B0%83&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E7%A9%BA%E8%B0%83&cid2=794&cid3=870&stock=1&wtype=1&page=2&s=29&scrolling=y&log_id=1540222666.30916&tpl=1_M&show_items=')
ktgetid = kongtiaospider.get_goodsid
ktwrite = kongtiaospider.write_to_mongodb

cpuspdier = Cpu('cpu','https://search.jd.com/search?keyword=cpu&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=cpu&cid2=677&cid3=678&wtype=1&page=1&s=61','https://search.jd.com/s_new.php?keyword=cpu&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=cpu&cid2=677&cid3=678&wtype=1&page=2&s=31&scrolling=y&log_id=1540222464.17737&tpl=1_M&show_items=')
cpugetid = cpuspdier.get_goodsid
cpuwrite = cpuspdier.write_to_mongodb

diskspider = Disk('disk','https://search.jd.com/search?keyword=%E7%A1%AC%E7%9B%98&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E7%A1%AC%E7%9B%98&cid2=677&cid3=683&wtype=1&page=1&s=1&click=0','https://search.jd.com/s_new.php?keyword=%E7%A1%AC%E7%9B%98&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E7%A1%AC%E7%9B%98&cid2=677&cid3=683&wtype=1&page=2&s=30&scrolling=y&log_id=1540473603.51149&tpl=1_M&show_items=')
diskgetid = diskspider.get_goodsid
diskwrite = diskspider.write_to_mongodb

threading.Thread(target=sjgetid).start()
threading.Thread(target=ktgetid).start()
threading.Thread(target=cpugetid).start()
threading.Thread(target=diskgetid).start()


for i in range(12):
    threading.Thread(target=sjwrite).start()
    threading.Thread(target=ktwrite).start()
    threading.Thread(target=cpuwrite).start()
    threading.Thread(target=diskwrite).start()

