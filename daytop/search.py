#!/bin/python
#coding:utf-8

import os
import sys
import requests
import json
import pdb
import traceback
import redis
import datetime
import time

from elasticsearch import Elasticsearch
from config import *


def parse_top30(day="2018-12-20"):
    es = Elasticsearch(ES_URI)
    dt = datetime.datetime.strptime(day,"%Y-%m-%d")
    start_t =  time.mktime(dt.timetuple())
    start_t = int(start_t)
    end_t = start_t + 3600*24
    queryJs = {
            "bool":{
                "must":{"match_all":{}},
                "filter":{
                    "range":{
                        "create_time":{
                            "gte": start_t,
                            "lte": end_t
                        }
                    }
                }
            }    
            
        }

    queryData = es.search(index=ES_INDEX_DY_POST,doc_type=ES_DOC_DY_POST,body={"query":queryJs,"size":30,"sort":{"statistics.digg_count":{"order":"desc"}}})
    #print queryData
    for item in queryData["hits"]["hits"]:
        #print item["_source"]["statistics"]["digg_count"]
        yield item["_source"]
    raise StopIteration

def dl_sv(url,fname,bdir="video"):
    if not os.path.exists(bdir):
        os.mkdir(bdir)
    sp = os.path.join(bdir,fname)
    req = requests.get(url)
    with open(sp,"wb") as fp:
        fp.write(req.content)
    print "Success save file:"+sp
    

def main():
    for rd in parse_top30("2018-12-21"):
        url = rd["play_addr"][0]
        fname = str(rd["aweme_id"]) + ".mp4"
        dl_sv(url,fname)


if __name__ == "__main__":
    main()
