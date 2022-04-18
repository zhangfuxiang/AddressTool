import os
import threading
import time

import pandas as pd
import wx

from GeoDecoder import GeoDecoder


class AddressTool(wx.Frame):

    def __init__(self):
        self.t = GeoDecoder()
        wx.Frame.__init__(self)
        wx.Frame.__init__(self, None, title='获取地址工具', size=(640, 480))
        self.SelBtn = wx.Button(self, label='选择文件', pos=(250, 5), size=(60, 25))
        self.SelBtn.Bind(wx.EVT_BUTTON, self.OnOpenFile)
        self.OkBtn = wx.Button(self, label='OK', pos=(320, 5), size=(80, 25))
        self.OkBtn.Bind(wx.EVT_BUTTON, self.ReadFile)
        self.ExportBtn = wx.Button(self, label='同步', pos=(405, 5), size=(80, 25))
        self.ExportBtn.Bind(wx.EVT_BUTTON, self.sync)
        self.ExportBtn = wx.Button(self, label='导出', pos=(500, 5), size=(80, 25))
        self.ExportBtn.Bind(wx.EVT_BUTTON, self.export)
        self.FileName = wx.TextCtrl(self, pos=(5, 5), size=(240, 25))
        self.FileContent = wx.TextCtrl(self, pos=(5, 35), size=(640, 480), style=(wx.TE_MULTILINE))
        self.curTime = time.strftime("%Y%m%d", time.localtime())  # 记录当前时间
        self.execF = False

    # 导出数据
    def export(self, event):
        self.t.export()

    # 同步数据
    def sync(self, event):
        def format(row):
            id = row["id"]
            lat = row["lat"]
            lon = row["lon"]
            createtime = row["createtime"]
            # sp, lac, ci = id.split("-")
            return {"id": id, "lat": lat, "lon": lon, "createtime": createtime}

        query = self.t.db[self.t.collection].find()
        crows = [format(i) for i in query]
        # crawled_set = set(self.t.db[self.t.collection].distinct("id"))
        crawled_set = set([i["id"] for i in self.t.db[self.t.collection].find()])
        self.t.logger.info("count is %s" % len(crows))
        self.t.logger.info("同步开始！")
        self.isThreads(self.t.is_switch, crows, crawled_set)
        self.t.logger.info("同步结束!")

    # 间隔时间
    def calc_time(self, timeStr):
        # timeStr = '2017-3-7 23:59:00'  时间格式
        timeArray = time.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
        # strptime 方法可以将一个时间字符串转成 struct_time
        timestamp = time.mktime(timeArray)
        # 用 mktime 转成 时间戳
        return int(time.time() - timestamp)

    # 选择文件
    def OnOpenFile(self, event):
        wildcard = 'All files(*.*)|*.*'
        dialog = wx.FileDialog(None, 'select', os.getcwd(), '', wildcard, style=wx.FD_OPEN | wx.FD_CHANGE_DIR)
        if dialog.ShowModal() == wx.ID_OK:
            self.FileName.SetValue(dialog.GetPath())
            dialog.Destroy

    # 读取文件
    def ReadFile(self, event):
        file = open(self.FileName.GetValue())
        self.FileContent.SetValue(file.read())
        file.close()
        df = pd.read_csv(self.FileName.GetValue(), sep=",")
        rows = df.to_dict(orient="records")
        # crawled_set = set(self.t.db[self.t.collection].distinct("id"))
        # crawled_set = set([i["id"] for i in t.db[self.t.collection].find()])
        crawled_set = set([i["id"] for i in self.t.db[self.t.collection].find()])
        self.t.logger.info("crawled is %s" % len(crawled_set))
        if len(rows) > self.t.maximum:
            self.t.logger.info("数量过大，启用定时任务，分配请求：")
            self.timer(3600, rows, crawled_set)
        else:
            self.isThreads(self.t.is_switch, rows, crawled_set)

        self.t.export()

    def timer(self, n, rows, crawled_set):
        num = int(len(rows) / self.t.maximum)
        i = 0
        self.curTime = time.strftime("%Y%m%d", time.localtime())  # 记录当前时间
        while True:
            self.t.logger.info("定时器监听一次！")
            if self.execF is False:
                self.t.logger.info(self.curTime)
                if i == num:
                    x = i * self.t.maximum
                    row = rows[x:]
                else:
                    x = i * self.t.maximum
                    y = (i + 1) * self.t.maximum
                    row = rows[x:y]
                self.execTask(row, crawled_set)  # 判断任务是否执行过，没有执行就执行
                i = i + 1
                self.execF = True
            else:  # 任务执行过，判断时间是否新的一天。如果是就执行任务
                desTime = time.strftime("%Y%m%d", time.localtime())
                if desTime > self.curTime:
                    self.execF = False  # 任务执行执行置值为
                    self.curTime = desTime

            if i == num + 1:
                self.t.logger.info("定时任务执行结束！")
                break
            else:
                time.sleep(n)

    def execTask(self, row, crawled_set):
        # 具体任务执行内容
        self.t.logger.info("执行一次定时任务!")
        self.isThreads(self.t.is_switch, row, crawled_set)

    # 是否开启多线程
    def isThreads(self, count, rows, crawled_set):
        if len(rows) > count:
            self.t.logger.info("所有线程开始！")
            # 线程数
            num = self.t.threads
            i = 0
            n = int(len(rows) / num)
            threads = []
            while i < num:
                if i + 1 == num:
                    x = i * n
                    row = rows[x:]
                else:
                    x = i * n
                    y = (i + 1) * n
                    row = rows[x:y]
                thread = threading.Thread(target=self.query, args=("thread_%s" % i, i, row, crawled_set))
                threads.append(thread)
                i = i + 1
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.t.logger.info("所有线程结束！")
        else:
            self.query("only", 0, rows, crawled_set)

    # 查询逻辑
    def query(self, thredName, num, rows, crawled_set):
        for row in rows:
            try:
                # sp, lac, ci = row["sp"], row["lac"], row["ci"]
                lat, lon = row["lat"], row["lon"]
                # if not t.check_duplicated(sp, lac, ci):
                # 新的基站查询地址
                if self.t.gen_id(lat, lon) not in crawled_set:
                    is_ok = self.t.fetech(thredName, num, lat, lon)
                    if (not is_ok) and (self.t.err_cnt > 10):
                        break
                else:
                    # 有坐标变化的基站更新地址
                    dict = {'id': self.t.gen_id(lat, lon)}
                    result = self.t.db[self.t.collection].find_one(dict)
                    lat1 = result["lat"]
                    lon1 = result["lon"]
                    if lat == lat1 and lon == lon1:
                        t = self.calc_time(result["createtime"])
                        if t < self.t.time:
                            continue
                        else:
                            self.t.db[self.t.collection].delete_one(dict)
                            self.t.logger.info("delete for %s" % dict)
                            is_ok = self.t.fetech(thredName, num, lat, lon)
                            if (not is_ok) and (self.t.err_cnt > 10):
                                break
                    else:
                        self.t.db[self.t.collection].delete_one(dict)
                        self.t.logger.info("delete for %s" % dict)
                        is_ok = self.t.fetech(thredName, num, lat, lon)
                        if (not is_ok) and (self.t.err_cnt > 10):
                            break
            except Exception as e:
                print(str(e))
        self.t.logger.info("%s线程结束！" % thredName)


if __name__ == '__main__':
    # 实例化一个主循环
    app = wx.App()
    SiteFrame = AddressTool()
    SiteFrame.Show()
    app.MainLoop()
