# encoding=utf8

# Created by zhangfx
import configparser
import logging
import os
import json
import threading

import requests
import pymongo
import pandas as pd
from datetime import datetime


class GeoDecoder:
    def __init__(self):
        self.session = requests.session()
        self.logger = self.gen_logger()
        self.err_cnt = 0
        self.meta = {}
        self.logger.info("加载配置文件：" + os.getcwd() + r"\config.ini")
        try:
            cf = configparser.ConfigParser()
            cf.read(os.getcwd() + r"/config.ini", encoding='UTF-8')
            self.logger.info('配置项：%s' % cf.options('config'))
        except Exception as e:
            self.logger.warning("配置文件加载报错,reason: %s" % e)
        host = cf.get("config", "mongo_host")
        port = int(cf.get("config", "mongo_port"))
        db_cli = pymongo.MongoClient(host=host, port=port)
        self.db = db_cli[cf.get("config", "mongo_dbname")]
        self.collection = cf.get("config", "mongo_collection")
        self.time = int(cf.get("config", "days") * 24 * 60 * 60)
        self.is_switch = int(cf.get("config", "is_switch"))
        self.is_files = int(cf.get("config", "is_files"))
        self.sep_location = int(cf.get("config", "sep_location"))
        if int(cf.get("config", "is_header")) == 0:
            self.is_header = False
        else:
            self.is_header = True
        self.threads = int(cf.get("config", "threads"))
        self.ak = cf.get("config", "ak")
        self.keys = self.ak.split(",")
        self.maximum = int(cf.get("config", "maximum"))
        self.gps_to_gao = 'https://restapi.amap.com/v3/assistant/coordinate/convert?locations={lon},{lat}&coordsys=gps&key={ak}'
        self.geo_url_fmt = 'https://restapi.amap.com/v3/geocode/regeo?location={lon},{lat}&roadlevel=1&key={ak}'
        self.session.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36"
        }
        self.logger.info("配置加载完成！")

    @staticmethod
    def gen_id(lat, lon):
        return "%s-%s" % (lat, lon)

    # 输出日志格式定义
    def gen_logger(self):
        fmt = '[%(asctime)s-%(levelname)s]:%(message)s'
        log_time_fmt = "%Y-%m-%d"
        logger_name = "GeoDecoder"
        logger = logging.getLogger(logger_name)
        if not os.path.exists("./log/"):
            os.mkdir("./log/")
        file_handler = logging.FileHandler("./log/%s_%s.log" % ("GeoDecoder", datetime.now().strftime(log_time_fmt)))
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.DEBUG)
        return logger

    def fetech(self, thredName, num, lat, lon):
        self.logger.info("%s fetech address for %s" % (thredName, self.gen_id(lat, lon)))
        id = self.gen_id(lat, lon)
        x = len(self.keys)
        if x > 1:
            n = int(num % x)
            key = self.keys[n]
        else:
            key = self.ak
        try:
            # gps坐标转换为高德坐标
            convertUrl = self.gps_to_gao.format(ak=key, lat=lat, lon=lon);
            self.logger.info("%s Access convertUrl: %s" % (thredName, convertUrl))
            json_convert = self.session.get(convertUrl, timeout=100).json()
            if json_convert["status"] != u'1':
                self.logger.warning("id[%s] respone error, status code: %s" % (id, json_convert["status"]))
                self.err_cnt += 1
                return False
            lon1 = json_convert["locations"].split(",")[0]
            lat1 = json_convert["locations"].split(",")[1]
            # 经纬度查询地址信息
            url = self.geo_url_fmt.format(ak=key, lat=lat1, lon=lon1)
            self.logger.info("%s Access url: %s" % (thredName, url))
            r = self.session.get(url, timeout=100)
            self.meta["r"] = r
            json_obj = r.json()
            if json_obj["status"] != u'1':
                self.logger.warning("id[%s] respone error, status code: %s" % (id, json_obj["status"]))
                self.err_cnt += 1
                return False
            json_obj["id"] = id
            json_obj["lat"] = lat
            json_obj["lon"] = lon
            json_obj["createtime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            str1 = json.dumps(json_obj)
            str2 = str1.replace("[]", "null")
            result = json.loads(str2)
            self.db[self.collection].insert_one(result)
            return True
        except Exception as e:
            self.logger.warning("id[%s] query failed, reason: %s" % (id, str(e)))

    def check_duplicated(self, sp, lac, ci):
        col = self.db[self.collection]
        id = self.gen_id(sp, lac, ci)
        result = col.find_one({"id": id})
        return True if result else False

    # 导出文件
    def export(self):
        count = self.db[self.collection].count_documents({})
        self.logger.info("导出开始！")
        if count > self.is_files:
            self.logger.info("分段导出开始！")
            num = int(count / self.is_files)
            i = self.sep_location
            # threads = []
            while i < num:
                if i + 1 == num:
                    query = self.db[self.collection].find().skip(i * self.is_files)
                else:
                    query = self.db[self.collection].find().skip(i * self.is_files).limit(self.is_files)

                self.exp_file("分段_%s" % i, i, query)
                # thread = threading.Thread(target=self.exp_file, args=("thread_%s" % i, i, query))
                # threads.append(thread)
                i = i + 1
            # for t in threads:
            #     t.start()
            # for t in threads:
            #     t.join()
            self.logger.info("所有分段导出结束！")
            self.logger.info("导出结束！")

        else:
            query = self.db[self.collection].find()
            self.exp_file("only", 0, query)
            self.logger.info("导出结束！")

    # 导出逻辑
    def exp_file(self, threadName, num, query):
        def format(row):
            id = row["id"]
            lat = row["lat"]
            lon = row["lon"]
            createtime = row["createtime"]
            address = row['regeocode']['formatted_address']
            address_comp = row['regeocode']["addressComponent"]
            province = address_comp["province"]
            city = address_comp["city"]
            citycode = address_comp["citycode"]
            district = address_comp["district"]
            towncode = address_comp["towncode"]
            township = address_comp["township"]
            adcode = address_comp["adcode"]
            self.logger.info("%s，id：%s，数据格式转换完成！" % (threadName, id))
            return {"id": id, "lat": lat, "lon": lon,
                    "createtime": createtime, "province": province, "city": city, "citycode": citycode,
                    "district": district, "towncode": towncode, "township": township,
                    "adcode": adcode, "address": address}

        self.logger.info("%s，导出开始！" % threadName)
        rows = [format(i) for i in query]
        df = pd.DataFrame(rows)
        if threadName == 'only':
            paths = os.getcwd() + r'/bsAddress_%s.txt' % (datetime.now().strftime("%Y%m%d%H%M"))
        else:
            paths = os.getcwd() + r'/bsAddress_%s_%s.txt' % (datetime.now().strftime("%Y%m%d%H%M"), num)

        df.to_csv(paths, index=False, header=self.is_header,
                  columns=["id", "lat", "lon", "createtime", "province", "city", "citycode",
                           "district", "towncode", "township", "adcode", "address"],
                  encoding="utf8")
        self.logger.info("%s，导出完成！" % threadName)
