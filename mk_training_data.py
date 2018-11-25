#!/usr/bin/env python3
import pandas as pd
import datetime
import numpy as np
from konlpy.tag import Komoran

def read_data(filepath):
    usecols = [
        'tweet'
    ]

    df = pd.read_csv(filepath,
        usecols=usecols,
        error_bad_lines=False,
        warn_bad_lines=True
        # encoding='utf-8'
    )
    return df

df1 = pd.read_csv('data/sentiment_training_data/subjectivity-polarity.csv',
    usecols=['ngram', 'max.value', 'max.prop']
)

df1 = df1.loc[df1['max.value'].isin(['POS', 'NEG'])]

# 형태소 분리
def get_tags(text):
    pos = komoran.pos(text)
    # str_pos = ";".join(map(str, pos))
    str_pos = ";".join(map("/".join, pos))
    return str_pos
    # return pos


df = read_data('data/tweet_test.csv')

df['tweet'] = df.tweet.astype(str)

komoran = Komoran()
for row in df.head().itertuples():
    print(row.index)
    word_str = get_tags(row.tweet)
    df.at[row.Index, 'word'] = word_str
    # df.set_value(row.Index, 'word', word_str)

print("word done....")
# ngram list 나누기
df1['ngram'] = df1['ngram'].str.split(';').tolist()
df['word'] = df['word'].str.split(';').tolist()

# max.prop 더한 값 넣을 새로운 column 만들기
df['prop'] = 0.0

def contains(small, big):
    for i in list(range(len(big)-len(small)+1)):
        for j in list(range(len(small))):
            if big[i+j] != small[j]:
                break
        else:
            return True
    return False

# # 나중에 head없애기
for index1, row1 in df.head().iterrows():
    sum = 0
    incld = True
    prop = 0
    for index, row in df1.iterrows():
        if(incld == True or len(row.ngram) == 1) :
            if(contains(row.ngram, row1.word)):       
                incld = True     
                if(row['max.value'] == 'POS'): # 긍정일 경우 더함
                    print("긍정" + str(row['max.prop']))
                    prop = row['max.prop']
                else: # 부정일 경우 뺌
                    print("부정!!" + str(-row['max.prop']))
                    prop = -row['max.prop']
            else:
                incld = False
                if(prop != 0):
                    sum += prop
                    prop = 0
    if(sum != 0):
        df.at[index1, 'prop'] = sum

# test
# print(df1.head(30))
# print(df.head(30))
print("ㅇㅕ기부터")
print(df['prop'].head())
print(df['word'].head())
print(len(df.index))