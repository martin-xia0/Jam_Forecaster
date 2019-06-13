#coding:utf-8
from pymongo import MongoClient
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import time
import json
import requests


class TrafficCollector:
    """
    collector raw traffic data in fixed intervals
    """
    def __init__(self):
        """
        initialize the query urls
        """
        self.client = MongoClient()
        self.db = self.client.jam_forecaster
        self.traffic = self.db.traffic
        self.key = 'ef7e204dda2d2279657fa85649588bb2'
        self.url = 'https://restapi.amap.com/v3/traffic/status/road?name={}&adcode=310000&key={}&extensions=all'
        self.locations = ["南北高架路","淮海中路","西藏南路","延安高架路","西藏中路","马当路","陕西南路","南昌路","北京东路","北京西路","南京西路","人民大道","福州路","武胜路","黄陂南路","合肥路","新闸路","武定路","昌化路","恒丰路","西藏中路","天目西路","天目中路","海宁路","沪太路","中兴路","大统路","秣陵路","恒丰路","西藏北路","共和新路","永兴路","虬江路","曲阜路","汉中路","长寿路","曲阜路","曲阜西路","恒通路","成都北路","内环高架路","昌化路","江宁路","澳门路","中潭路","芷江西路","普善路","柳营路","洛川中路","延长中路","洛川路","新村路","广西中路","志丹路","岚皋路","常德路","光复西路","宜昌路","陕西北路","安远路","海防路","昌平路","昌化路","武定路","新闸路","广东路","大沽路","河南中路","福建中路","七浦路","浙江北路","河南北路","天目东路","宝山路","会文路","东宝兴路","青云路","景祥路","广中路","同心路","株洲路","洛川中路"]        
        self.urls = [self.url.format(road, self.key) for road in self.locations]
        print(self.urls)

    def get_traffic(self, i):
        """
        do one time traffic query and save to database
        :return: None
        """
        traffic_data = []
        for url in self.urls:
            r = requests.get(url)
            # print(r.status_code)
            response = json.loads(r.text)
            # print(response)
            if response['infocode'] == '10000':
                try:
                    for road in response['trafficinfo']['roads']:
                        data = {}
                        data['i'] = i
                        data['identity'] = road['name']+road['angle']+road['lcodes']
                        t = time.localtime()
                        data['date'] = (t.tm_mon, t.tm_mday)
                        data['time'] = (t.tm_hour, t.tm_min)
                        data['speed'] = road['speed']
                        traffic_data.append(data)
                except:
                    pass
        print("二号引擎完成交通数据采集 {} 条".format(len(traffic_data)))
        return traffic_data

def start_engine():
    """
    start the data engine, add jobs to the scheduler
    :return:
    """
    traffic = TrafficCollector()
    # data = traffic.get_traffic()
    # print(data)
    # print(len(data))
    # with open('location_3.json', 'a', encoding='utf-8') as f:
    #     for d in data:
    #         locations = json.dump(d, f, ensure_ascii=False)
    client = MongoClient()
    db = client.jam_forecaster
    i = [1]
    scheduler = BlockingScheduler()
    scheduler.add_job(get_data, trigger='cron', minute='*/1', second='10', max_instances=10, args=[traffic, db, i])
    scheduler.start()


def get_data(traffic, db, i):
    """
    get data for every 1 minuets
    :return: 
    """
    traffic_data = traffic.get_traffic(i[0])
    db.traffic_3.insert_many(traffic_data)
    print("第 {} 次采集，于 {} 完成 {} 条数据爬取及存储".format(i[0], datetime.now(), len(traffic_data)))
    print('三号引擎当前交通数据条数 {}'.format(db.traffic_3.count_documents({})))
    i[0] = i[0] + 1


if __name__ == '__main__':
    start_engine()