#coding:utf-8
from pymongo import MongoClient
import time

client = MongoClient('localhost', 27017)
db = client.jam_forecaster

# #总量
# print('当前交通数据条数 {}'.format(db.traffic.count_documents({})))
# print('当前天气数据条数 {}'.format(db.weather.count_documents({})))


# #今日爬取量
# t = time.localtime()
# date = (t.tm_mon, t.tm_mday)
# print('今日交通数据条数 {}'.format(db.traffic.count_documents({"date": date})))
# print('今日天气数据条数 {}'.format(db.weather.count_documents({"date": date})))

#总量
print('二号引擎当前交通数据条数 {}'.format(db.traffic_3.count_documents({})))


# #今日爬取量
# t = time.localtime()
# date = (t.tm_mon, t.tm_mday)
# print('二号引擎今日交通数据条数 {}'.format(db.traffic_3.count_documents({"date": date})))


# 统计频率用

# # 把所有边打出来
piece = db.traffic_3.find()

# for i in piece:
#     print(i)
location_dict = {}
for i in piece:
    location = i["identity"]
    location_dict[location] = location_dict.get(location, 0) + 1
"""

# #抓一个看
# piece = db.traffic_3.find({'identity': '内环高架路226-928,-22,-21,-20', 'date': [5, 30]})
# # for i in piece:
# #     print(i['speed'])

# with open('./neihuan_road_data_5_30.txt', 'w+') as f:
#     for i in piece:
#         speed = i['speed']
#         print(i)
#         f.write(str(i)+'\n')
"""
items = list(location_dict.items())
print(len(items))
        # sort the word frequency
items.sort(key=lambda x: x[1], reverse=True)
items = items[:1000]
# locations = [i[0] for i in items]

with open('./locations.txt', 'w+') as f:
    f.write(str(items))

"""

piece = db.traffic_3.find({"identity":'昌化路302-3628,-3627,-3621'})
items = list(piece)
items.sort(key=lambda x: x['i'], reverse=True)
print(items[:50])
"""