#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import multiprocessing
import twint
from datetime import date, timedelta

# 이전 파일 삭제 
def remove_file(folder_name):
    file = "/home/maria_dev/PresidentMoon-analysis/data/scraping/" + folder_name + "/tweets.csv"
    if os.path.isfile(file):
        os.remove(file)

# args[0] = 검색할 텍스트  args[1] = 폴더명
def scraping(args):
    c = twint.Config()
    c.Search = args[0]
    c.Lang = "ko"
    c.Since = before_yesterday.strftime('%Y-%m-%d')
    c.Until = yesterday.strftime('%Y-%m-%d')
    c.Timedelta = 1
    c.Store_csv = True
    c.Output = "/home/maria_dev/PresidentMoon-analysis/data/scraping/" + args[1]
    twint.run.Search(c)

if __name__ == '__main__':
    [remove_file(folder_name) for folder_name in ["moon", "unification", "dprk"]]

    # 크롤링할 어제 날짜 구하기
    yesterday = date.today() - timedelta(1)
    before_yesterday = yesterday - timedelta(1)

    pool = multiprocessing.Pool(processes=3)
    pool.map(scraping, [["문재인", "moon"], ["통일", "unification"], ["북한", "dprk"]])