#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import twint
import os
from datetime import date, timedelta

# 이전 파일 삭제 
# file = 'hdfs:///user/maria_dev/data/tweets.csv'
# if os.path.isfile(file):
#     os.remove(file)

# 크롤링할 어제 날짜 구하기
yesterday = date.today() - timedelta(1)
before_yesterday = yesterday - timedelta(1)

c = twint.Config()
c.Search = "문재인"
c.Lang = "ko"
c.Since = before_yesterday.strftime('%Y-%m-%d')
c.Until = yesterday.strftime('%Y-%m-%d')
c.Timedelta = 1
c.Store_csv = True
c.Output = "/home/maria_dev/data"

twint.run.Search(c)