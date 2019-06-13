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
        self.positions = np.load("positions.npy")
        # 二次遍历，得到全局数据
        for d in self.date_list:
            # 当天数据清洗
            self.aggregate_data_2(d)
        # print(self.global_data)
        # 最终导出数据
        np.save("data_graph.npy", np.array(self.global_data_x))
        # np.save("data_y.npy", np.array(self.global_data_y))
        # print("data size {}".format(np.shape(global_data_x)))
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
        
        filtered_data_x = {}
        # 选取合格的时间点
        for minute in raw_data.keys():
            if len(raw_data[minute]) == len(self.positions):
                filtered_data_x[minute] = raw_data[minute]

        filtered_minutes = list(filtered_data_x.keys())
        filtered_minutes.sort()

        # 开始遍历
        for minute in filtered_minutes:
            x = []
            for position in self.positions:
                x.append(filtered_data_x[minute][position])
            x_arr = np.array(x)
            self.global_data_x.append(x_arr)

        np.save("data_graph.npy", np.array(self.global_data_x))
        print("当前大小{}".format(np.array(self.global_data_x).shape))
        # 清理内存
        return 

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.run()
