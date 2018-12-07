#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import multiprocessing
import twint
from datetime import date, timedelta

# 이전 파일 삭제 
def remove_file():
    moon_file = '/home/maria_dev/PresidentMoon-analysis/data/scraping/moon/tweets.csv'
    unification_file = '/home/maria_dev/PresidentMoon-analysis/data/scraping/unification/tweets.csv'
    if os.path.isfile(moon_file):
        os.remove(moon_file)
    if os.path.isfile(unification_file):
        os.remove(unification_file)

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
    remove_file()

    # 크롤링할 어제 날짜 구하기
    yesterday = date.today() - timedelta(1)
    before_yesterday = yesterday - timedelta(1)

    pool = multiprocessing.Pool(processes=2)
    pool.map(scraping, [["문재인", "moon"], ["통일", "unification"]])