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

