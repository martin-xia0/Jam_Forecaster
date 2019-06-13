import json
import requests
import pandas as pd

class Graph:
    """
    collector raw traffic data in fixed intervals
    """
    def __init__(self):
        """
        initialize the query urls
        """
        self.key = 'ef7e204dda2d2279657fa85649588bb2'
        self.dist_url = 'https://restapi.amap.com/v3/distance?origins={}&destination={}&key=ef7e204dda2d2279657fa85649588bb2'
        self.point_url = 'https://restapi.amap.com/v3/geocode/geo?key=ef7e204dda2d2279657fa85649588bb2&address={}&city=上海'
        # self.urls = {l_id: self.url.format(self.locations[l_id], self.locations[], self.key) for l_id in self.locations.keys()}
        # print(self.urls)
        self.points = []
        self.points_str = []
        self.graph = []
        self.locations = ['昌化路', '昌化路', '曲阜路', '曲阜路', '武定路', '西藏中路', '武定路', '恒丰路', '恒丰路', '西藏中路', '新闸路', '新闸路', '大统路', '中潭路', '中潭路', '海防路', '海防路', '昌平路', '昌平路', '福建中路', '天目东路', '天目东路', '会文路', '会文路', '长寿路', '天目西路', '宝山路', '河南北路', '河南北路', '宝山路', '虬江路', '浙江北路', '天目中路', '河南中路', '长寿路', '浙江北路', '恒通路', '宜昌路', '天目西路', '恒通路', '虬江路', '安远路', '海宁路']

    def generate_points(self):
        for location in self.locations:
            url = self.point_url.format(location)
            r = requests.get(url)
            print(r.text)
            point = json.loads(r.text)['geocodes'][0]['location']
            p = point.split(',')
            self.points.append({"name": location, "point": p})
        print(self.points)

    def generate_points_str(self):
        for location in self.locations:
            url = self.point_url.format(location)
            r = requests.get(url)
            # print(r.text)
            point = json.loads(r.text)['geocodes'][0]['location']
            self.points_str.append(point)
        print(self.points_str)

    def generate_graph(self):
        """
        get the location graph
        :return:
        """
        for x in self.points_str:
            row = []
            for y in self.points_str:
                url = self.dist_url.format(x, y)
                r = requests.get(url)
                dist = int(json.loads(r.text)['results'][0]['distance'])
                row.append(dist)
                print("{} 到 {}的距离 {} ".format(x, y, dist))
            self.graph.append(row)
            print("完成{}".format(x))
        df = pd.DataFrame(self.graph)
        print(df)
        print(df.shape)
        df.to_csv("W_43_dist.csv", header = None, index = None)


if __name__ == "__main__":
    g = Graph()
    g.generate_points_str()
    g.generate_graph()