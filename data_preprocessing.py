#!/usr/bin/env python3
import pandas as pd
import datetime
import numpy as np
# from konlpy.tag import Hannanum
# from konlpy.tag import Okt
from konlpy.tag import Komoran
from konlpy.tag import Twitter
from collections import Counter
from nltk.classify import NaiveBayesClassifier

def read_data(filepath):
    # usecols = [
    #     'id', 'conversation_id', 'date', 'time',
    #     'username', 'name', 'tweet'
    # ]
    usecols = [
        'date', 'time', 'tweet'
    ]
    # dtype = {
    #     'id':
    # }

    df = pd.read_csv(filepath,
        # usecols=usecols,
        parse_dates=[['date', 'time']],
        dtype={
            'username': str
        },
        error_bad_lines=False,
        warn_bad_lines=True
        # encoding='utf-8'
    )
    return df

# def preprocess_text(text):



# 형태소 분리
# def get_tags(text):
#     nouns = h.nouns(text)

#     # count = Counter(nouns)
#     # print(nouns)
#     str_nouns = "|".join(nouns)
#     return str_nouns
def get_tags(text):
    pos = komoran.pos(text)
    # str_pos = ";".join(map(str, pos))
    str_pos = ";".join(map("/".join, pos))
    return str_pos

def get_sentiment(text):
    s = classifier.classify(text)
    return s

def get_nouns(text):
    twitter = Twitter()
    nouns = twitter.nouns(text)
    return nouns

df = read_data('./data/tweet_test.csv')

# 중복 제거
df = df.drop_duplicates()

# date_time에 맞지 않는 데이터 삭제
df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
df['id'] = pd.to_numeric(df['id'], errors='coerce')
df['conversation_id'] = pd.to_numeric(df['conversation_id'], errors='coerce')

# 시간 조정
df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)

# tweet column 타입 string으로 변경 & 소문자로 변경
df['tweet'] = df.tweet.astype(str)
df['tweet'] = df['tweet'].apply(lambda x: x.lower())
 
# hashtag 분리
df['hashtag'] = df['tweet'].str.findall(r'#.*?(?=\s|$)')
for row in df.itertuples():
    df.at[row.Index, 'hashtag'] = "|".join(row.hashtag)

# mention 분리
df['mention'] = df['tweet'].str.findall(r'@.*?(?=\s|$)')
for row in df.itertuples():
    df.at[row.Index, 'mention'] = "|".join(row.mention)

# 단어별로 자른 것 넣을 새로운 column 만들기
df['word'] = ''

# 명사별로 자르고 word column에 값 넣기
# head로 설정해놔서 나중에 없애고 다시 테스트
# komoran = Komoran()
# for row in df.head().itertuples():
#     word_str = get_tags(row.tweet)
#     df.at[row.Index, 'word'] = word_str
    # df.set_value(row.Index, 'word', word_str)

# 트위터 클래스 변경
for row in df.itertuples():
    word_str = get_nouns(row.tweet)
    df.at[row.Index, 'word'] = word_str

# 결측값 있을 시 제거
# df['id'].dropna()

print(df['word'])

# csv 저장
df.to_csv("./data/result.csv", mode="w")


# test
# print(df.dtypes)
 
# print(df['new'].head())
# print(get_tags(df['tweet'].head()))

# df['tweet1'] = pd.Series(get_tags(str(df['tweet'])))
# print(get_tags(str(df['tweet'])))

# okt = Okt()
# list_d = okt.pos(str(df['tweet']))
# df2 = pd.DataFrame()
# print(list_d)

# def tokenize(doc):
#     return ['/'.join(t) for t in pos_tagger.pos(doc, norm=True, stem=True)]
# train_docs1 = [(tokenize(row[1]), row[2]) for row in train_data]
# print(train_docs1)