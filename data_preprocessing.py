#-*- coding: utf-8 -*-
import pandas as pd
import datetime
import numpy as np
import re
import multiprocessing
import os
from hanspell import spell_checker
from konlpy.tag import Hannanum

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# 이전 파일 삭제 
def remove_file(folder_name):
    file = "/home/maria_dev/PresidentMoon-analysis/data/preprocessing/" + folder_name + "/clean_data.csv"
    if os.path.isfile(file):
        os.remove(file)

# 파일 읽기
def read_data(filepath):
    df = pd.read_csv(filepath,
        parse_dates=[['date', 'time']],
        dtype={
            'username': str
        },
        error_bad_lines=False,
        warn_bad_lines=True,
    )
    return df

# 명사, 용언 추출
def extract_pos(text):
    h = Hannanum()
    pos = h.pos(text, ntags=22, flatten=True)
    pos_list = [item for item in pos if item[1] == 'NC' or item[1] == 'NQ' or item[1] == 'NN' or item[1] == 'PV' or item[1] == 'PA']
    dct = dict(pos_list)
    for stopword in stopwords.itertuples(): # 불용어 체크
        if dct.get(stopword._1):
            del dct[stopword._1]
    split_pos = "|".join("%s,%s" % tup for tup in dct.items())
    return split_pos

# 전처리
def preprocess(folder_name):
    df = read_data('/home/maria_dev/PresidentMoon-analysis/data/scraping/' + folder_name + "/tweets.csv")

    # 중복 제거
    df = df.drop_duplicates()

    # date_time에 맞지 않는 데이터 삭제
    df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['conversation_id'] = pd.to_numeric(df['conversation_id'], errors='coerce')

    # UTC 시간 조정
    df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)
    df['date'] = df['date_time'].dt.date

    # tweet column 타입 string으로 변경 & 소문자로 변경
    df['tweet'] = df.tweet.astype(str)
    df['tweet'] = df['tweet'].apply(lambda x: x.lower())

    # 단어별로 자른 것 넣을 새로운 column 만들기
    df['word'] = ''

    hangul = re.compile("[^"
                    u"\U0000AC00-\U0000D7AF"
                    "]+", flags=re.UNICODE)

    sum = 0

    for row in df.itertuples():
        sum = sum + 1
        print(sum)
        tweet = str(row.tweet).decode('utf-8', errors='replace')
        hangul_text = re.sub(hangul, ' ', tweet)
        if(hangul_text.isspace() == False):
            try:
                spell_ok = spell_checker.check(str(hangul_text))
            except ValueError:
                word_str = extract_pos(str(hangul_text))
            else:
                word_str = extract_pos(str(spell_ok.checked))
            df.at[row.Index, 'word'] = word_str

    # csv 저장
    df[['date', 'word']].to_csv("/home/maria_dev/PresidentMoon-analysis/data/preprocessing/" + folder_name + "/clean_data.csv", mode="w")


if __name__ == '__main__':
    [remove_file(folder_name) for folder_name in ["moon", "unification", "dprk"]]

    stopwords = pd.read_json('/home/maria_dev/PresidentMoon-analysis/data/stopwords/stopwords_ko.json')

    pool = multiprocessing.Pool(processes=4)
    pool.map(preprocess, ["moon", "unification", "dprk"])

