#coding:utf-8
from data_engine import TrafficCollector, WeatherController
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from pymongo import MongoClient


def start_engine():
    """
    start the data engine, add jobs to the scheduler
    :return:
    """
    traffic = TrafficCollector()
    weather = WeatherController()
    client = MongoClient()
    db = client.jam_forecaster

    scheduler = BlockingScheduler()
    scheduler.add_job(get_data, trigger='cron', hour='6-22', minute='*/5', second='0', max_instances=10, args=[traffic, weather, db])
    scheduler.start()


def get_data(traffic, weather, db):
    """
    get data for every 5 minuets
    :return: 
    """
    traffic_data = traffic.get_traffic()
    weather_data = weather.get_weather()
    db.traffic.insert_many(traffic_data)
    db.weather.insert_many(weather_data)
    print('于 {} 完成数据爬取及存储'.format(datetime.now()))
    print('当前交通数据条数 {}'.format(db.traffic.count_documents({})))
    print('当前天气数据条数 {}'.format(db.weather.count_documents({})))


if __name__ == '__main__':
    start_engine()