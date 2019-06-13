# coding:utf-8
from pymongo import MongoClient
import time
import json
import requests
import pandas as pd
import numpy as np


class DataCleaner():
    """
    数据清洗器
    """

    def __init__(self):
        """
        初始化
        """
        self.client = MongoClient()
        self.db = self.client.jam_forecaster
        self.table = self.db.traffic_2
        # 全局可用地点集
        self.positions = []
        # 全局可用时间集
        self.time_points = {}
        # 全局清洗结果
        self.global_data_x = []
        self.global_data_y = []

        # 初始化日期遍历list
        self.date_list = [(4, d) for d in range(19, 30)]+[(5, 1), (5, 3)]
        # 初始化时分遍历list
        self.time_list = [(m, s) for m in range(6, 22) for s in range(0, 60)]

    def run(self):
        """
        清理器主程序
        """
        """
        # 初次遍历，得到全局地点集和全局时间集
        for d in self.date_list:
            # 有效时间列表初始化
            self.time_points[d] = []
            for t in self.time_list:
                # 一轮过滤
                self.filter(d, t)
            print("{}天留下了{}个时间点".format(d, len(self.time_points[d])))
        print("完成一轮遍历，地点个数{}".format(len(self.positions)))
        np.save("positions.npy", np.array(self.positions))
        np.save("days.npy", self.time_points.keys())
        np.save("times.npy", self.time_points.values())
        """
        self.positions = np.load("positions.npy")
        # 二次遍历，得到全局数据
        for d in self.date_list:
            # 当天数据清洗
            self.aggregate_data_2(d)
        # print(self.global_data)
        # 最终导出数据
        np.save("data_x.npy", np.array(self.global_data_x))
        np.save("data_y.npy", np.array(self.global_data_y))
        # print("data size {}".format(np.shape(global_data_x)))
        return

    def filter(self, d, t):
        """
        道路清洗过滤器
        :return:
        """
        print("扫描{} {}".format(d, t))
        rows = list(self.table.find({'date': d, 'time': t}))
        if len(rows) >= 50:
            # 有效时间点位，将此时间加入全局时间
            self.time_points[d].append(t)
            # 进行道路点位过滤
            # 三个参数唯一确定一个数据点
            positions = ["{}/{}/{}".format(row["road"], row["angle"], row["position_id"]) for row in rows]
            if not self.positions:
                self.positions = positions
            else:
                for point in self.positions:
                    if point not in positions:
                        # 去除非全局道路点
                        print("去除{}".format(point))
                        self.positions.remove(point)
        else:
            pass
        print("还剩{}个点".format(len(self.positions)))
        # 清理内存
        del rows
        return



    def aggregate_data_2(self, d):
        """
        将同一时间点的数据聚拢，每次合并都进行两个维度的过滤
        1.向前20分钟有数据缺失的，将该事件过滤（时间维）
        2.只过滤存在于全局地点集的地点（地点维）
        找出始终存在的地点集(list)包含三个参量“id”、“road”和“angle”
        """
        print("开始{}".format(d))
        # 初始化有效粗数据集
        raw_data = {}
        for t in self.time_list:
            minute = t[0]*60+t[1]
            raw_data[minute] = {}
        # print(self.positions)
        # 当天有效分钟值
        day_minutes = [t[0]*60+t[1] for t in self.time_list]

        # 从当日全局原始数据提取粗数据集
        day_data = list(self.table.find({"date": d}))
        for row in day_data:
            t = (row["time"][0], row["time"][1])
            minute = t[0]*60+t[1]
            position = "{}/{}/{}".format(row["road"], row["angle"], row["position_id"])
            # print("{}{}".format(t, position))
            if minute in day_minutes and position in self.positions:
                raw_data[minute][position] = row["speed"]
        # print(raw_data)
        
        # 当天有效y值序列
        raw_y_data = {}
        day_y_data = list(self.table.find({"date": d, "road":"天目中路", "angle":"192", "position_id":"1"}))
        for row in day_y_data:
            t = (row["time"][0], row["time"][1])
            minute = t[0]*60+t[1]
            if minute in day_minutes:
                raw_y_data[minute] = row["speed"]
        # print(raw_y_data)

        filtered_data_x = {}
        # 选取合格的时间点
        for minute in raw_data.keys():
            if len(raw_data[minute]) == len(self.positions) and minute in raw_y_data.keys():
                filtered_data_x[minute] = raw_data[minute]

        filtered_minutes = list(filtered_data_x.keys())
        filtered_minutes.sort()

        # 数据采集
        d_global_data_x = []
        d_global_data_y = []
        # 开始遍历
        j = 0
        # print("可研究点{}".format(len(filtered_minutes)))
        for i in range(20, len(filtered_minutes)-20):
            minute_now = filtered_minutes[i]
            if minute_now-19 == filtered_minutes[i-19]:
                j+=1
                print(j)
                # 构造X集
                x = []
                for minute in range(minute_now-19, minute_now+1):
                    x_row = []
                    for position in self.positions:
                        x_row.append(raw_data[minute][position])
                    x.append(x_row)
                x_arr = np.array(x)
                # print("当前点位x的大小{}".format(x_arr.shape))
            else:
                continue

            # 判断向后20min是否存在
            minute_future = minute_now+20
            if minute_future in raw_y_data.keys():
                # 构造Y
                y = raw_y_data[minute_future]
                # print("当前点位y的大小{}".format(y))
            else:
                continue
            # key = "{}/{}/{}/{}".format(d[0],d[1],t_now[0],t_now[1])
            d_global_data_x.append(x_arr)
            d_global_data_y.append(y)
        print("{} 数据集大小{}".format(d, len(d_global_data_y)))
        # 暂存数据
        self.global_data_x += d_global_data_x
        self.global_data_y += d_global_data_y
        np.save("data_x.npy", np.array(self.global_data_x))
        np.save("data_y.npy", np.array(self.global_data_y))
        # 清理内存
        del day_data
        del raw_data
        return 

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()
