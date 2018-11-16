#!/usr/bin/env python3
import pandas as pd
import datetime
import numpy as np
from konlpy.tag import Hannanum
from konlpy.tag import Okt
from collections import Counter
from nltk.classify import NaiveBayesClassifier

def read_data(filepath):
    # usecols = [
    #     'id', 'conversation_id', 'date', 'time',
    #     'username', 'name', 'tweet'
    # ]

    df = pd.read_csv(filepath,
        # usecols=usecols,
        parse_dates=[['date', 'time']],
        # dtype={
        #     'id': pd.np.float64
        # },
        # encoding='utf-8'
    )
    return df

# 명사 추출
# def get_tags(text, ntags=50, multiplier=10):
def get_tags(text):
    h = Hannanum()
    nouns = h.nouns(str(text))

    # count = Counter(nouns)
    # print(nouns)
    return str_nouns

df = read_data('data/tweet_test.csv')

# date_time에 맞지 않는 데이터 삭제
df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')

# 시간 조정
df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)

