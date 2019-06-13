#coding:utf-8
from pymongo import MongoClient
import time
import json
import requests


def build_database():
    """
    build database for weather and traffic data
    :return: 
    """
    pass


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
        with open('location.json', 'r', encoding='utf-8') as f:
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
        print("完成交通数据采集 {} 条".format(len(traffic_data)))
        return traffic_data


class WeatherController:
    """
    collector raw traffic data in fixed intervals
    """
    def __init__(self):
        """
        initialize the query urls
        """
        self.client = MongoClient()
        self.db = self.client.jam_forecaster
        self.weather = self.db.weather
        self.url = 'http://aliv8.data.moji.com/whapi/json/aliweather/condition'
        self.appcode = '33a790e6874c487f872185587b67dd2e'
        self.areas = self.load_areas()
        self.token = {'token': 'ff826c205f8f4a59701e64e9e64e01c4'}
        self.header = {'Authorization': 'APPCODE 33a790e6874c487f872185587b67dd2e',
                       'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
        self.bodys = {a_id: dict(self.areas[a_id], **self.token) for a_id in self.areas.keys()}
        print(self.bodys)

    @staticmethod
    def load_areas():
        """

        :return:
        """
        with open('weather.json','r', encoding='utf-8') as f:
            areas = json.load(f)
            print(areas)
        return areas

    def get_weather(self):
        """

        :return:
        """
        weather_data = []
        for id in self.bodys.keys():
            body = self.bodys[id]
            # print(body)
            r = requests.post(self.url, data=body, headers=self.header)
            response = json.loads(r.text)
            # print(response)
            if response['msg'] == 'success':
                data = {}
                data['area_name'] = response['data']['city']['name']
                data['area_id'] = id
                t = time.localtime()
                data['date'] = (t.tm_mon, t.tm_mday)
                data['time'] = (t.tm_hour, t.tm_min)
                condition = response['data']['condition']
                data['condition'] = condition['condition']
                data['condition_id'] = condition['conditionId']
                data['humidity'] = condition['humidity']
                data['pressure'] = condition['pressure']
                data['temp'] = condition['temp']
                data['temp_feel'] = condition['realFeel']
                data['wind_dir'] = condition['windDir']
                data['wind_level'] = condition['windLevel']
                print(data)
                weather_data.append(data)
        print("完成天气数据采集 {} 条".format(len(weather_data)))
        return weather_data


if __name__ == '__main__':
    traffic = TrafficCollector()
    traffic.get_traffic()
    weather = WeatherController()
    weather.get_weather()