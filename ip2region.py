# Copyright 2022 The Ip2Region Authors. All rights reserved.
# Use of this source code is governed by a Apache2.0-style
# license that can be found in the LICENSE file.
#

from xdbSearcher import XdbSearcher
import csv
import os

def searchWithFile():
    # 1. 创建查询对象
    dbPath = "../../data/ip2region.xdb"
    searcher = XdbSearcher(dbfile=dbPath)
    
    # 2. 执行查询
    ip = "1.2.3.4"
    region_str = searcher.searchByIPStr(ip)
    print(region_str)
    
    # 3. 关闭searcher
    searcher.close()

def searchWithVectorIndex():
     # 1. 预先加载整个 xdb
    dbPath = "../../data/ip2region.xdb"
    vi = XdbSearcher.loadVectorIndexFromFile(dbfile=dbPath)

    # 2. 使用上面的缓存创建查询对象, 同时也要加载 xdb 文件
    searcher = XdbSearcher(dbfile=dbPath, vectorIndex=vi)
    
    # 3. 执行查询
    ip = "1.2.3.4"
    region_str = searcher.search(ip)
    print(region_str)

    # 4. 关闭searcher
    searcher.close()
    
def searchWithContent():
    # 1. 预先加载整个 xdb
    dbPath = "../../data/ip2region.xdb";
    cb = XdbSearcher.loadContentFromFile(dbfile=dbPath)
    
    # 2. 仅需要使用上面的全文件缓存创建查询对象, 不需要传源 xdb 文件
    searcher = XdbSearcher(contentBuff=cb)

    # filename = "./query-presto-39167.csv"
    # with open(filename,'r',encoding='UTF-8') as f:
    #     render = csv.reader(f)  # reader(迭代器对象)--> 迭代器对象
    #         # 取表头
    #     header_row = next(render)
    #     # print(render)
    #     for row in render:
    #         ip = row[6]
    #         region_str = searcher.search(ip)
    #         print(region_str)
    #         exit()
    f = csv.reader(open("./query-presto-39167.csv",'r'))
    wf = open('./query-presto-39167-2.csv','w')
    csv_write = csv.writer(wf)
    
    header_row = next(f)
    for row in f:
        ip = row[6]
        # print(ip)
        ip = ip.split(',')
        # exit()
        region_str = searcher.search(ip[0])
        print(region_str)
        row[7]=region_str
        csv_write.writerow(row)
    wf.close()
    #linux下
    # os.system('mv point2.csv point.csv')
    
    # 3. 执行查询
    # ip = "1.2.3.4"
    # region_str = searcher.search(ip)
    # print(region_str)

    # 4. 关闭searcher
    searcher.close()
    

# 离线IP地址定位库和IP定位数据管理框架，10微秒级别的查询效率，定位csv中的ip地址，并生成新的文件
if __name__ == '__main__':
    searchWithVectorIndex()
    
