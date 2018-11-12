#!/usr/bin/env python3

import pandas as pd
import datetime
from konlpy.tag import Hannanum
from collections import Counter

// 명사 추출
def get_tags(text, ntags=50, multiplier=10):
    h = Hannanum()
    nouns = h.nouns(text)
    count = Counter(nouns)
    return nouns

df = pd.read_csv('data/tweet_test.csv',
    usecols=['id', 'conversation_id', 'date', 'time', 'username', 'name', 'tweet'],
    # usecols=['date', 'time'],
    parse_dates=[['date', 'time']],
    # dtype={
    #     'id': pd.np.float64
    # },
    # encoding='utf-8'
)
df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)
print(get_tags(str(df['tweet'].head())))
