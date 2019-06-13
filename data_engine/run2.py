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
        self.url = 'https://restapi.amap.com/v3/traffic/status/circle?location={}&key={}&extensions=all&radius=10'
        self.locations = self.load_location()
        self.urls = {l_id: self.url.format(self.locations[l_id], self.key) for l_id in self.locations.keys()}
        print(self.urls)

    @staticmethod
    def load_location():
        """
        load location coordinate from json
        :return: coordinate set
        """
        with open('location_2.json', 'r', encoding='utf-8') as f:
            locations = json.load(f)
            print(locations)
        return locations

    def get_traffic(self):
        """
        do one time traffic query and save to database
        :return: None
        """
        traffic_data = []
        for position_id in self.urls:
            url = self.urls[position_id]
            r = requests.get(url)
            # print(r.status_code)
            response = json.loads(r.text)
            # print(response)
            if response['infocode'] == '10000':
                try:
                    for road in response['trafficinfo']['roads']:
                        data = {}
                        data['road'] = road['name']
                        t = time.localtime()
                        data['position_id'] = position_id
                        data['date'] = (t.tm_mon, t.tm_mday)
                        data['time'] = (t.tm_hour, t.tm_min)
                        data['lcodes'] = road['lcodes']
                        data['angle'] = road['angle']
                        data['direction'] = road['direction']
                        data['speed'] = road['speed']
                        data['condition'] = response['trafficinfo']['evaluation']
                        data['status'] = road['status']
                        print(data)
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
    client = MongoClient()
    db = client.jam_forecaster

    scheduler = BlockingScheduler()
    scheduler.add_job(get_data, trigger='cron', hour='6-22', minute='*/1', second='10', max_instances=10, args=[traffic, db])
    scheduler.start()


def get_data(traffic, db):
    """
    get data for every 5 minuets
    :return: 
    """
    traffic_data = traffic.get_traffic()
    db.traffic_2.insert_many(traffic_data)
    print('于 {} 完成数据爬取及存储'.format(datetime.now()))
    print('二号引擎当前交通数据条数 {}'.format(db.traffic_2.count_documents({})))


if __name__ == '__main__':
    start_engine()