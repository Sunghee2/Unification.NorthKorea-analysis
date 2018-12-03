#-*- coding: utf-8 -*-
import pandas as pd
import datetime
import numpy as np
import re
from hanspell import spell_checker
from konlpy.tag import Hannanum
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def read_data(filepath):
    df = pd.read_csv(filepath,
        parse_dates=[['date', 'time']],
        dtype={
            'username': str
        },
        error_bad_lines=False,
        warn_bad_lines=True
    )
    return df

def get_nouns(text):
    pos = h.pos(text, ntags=22, flatten=True)
    nouns = [item for item in pos if item[1] == 'NC' or item[1] == 'NQ' or item[1] == 'NN' or item[1] == 'PV' or item[1] == 'PA']
    dct = dict(nouns)
    for stopword in stopwords.itertuples(): # 불용어 체크
        if dct.get(stopword._1):
            del dct[stopword._1]
    split_nouns = "|".join("%s,%s" % tup for tup in dct.items())
    return split_nouns

df = read_data('./data/tweets-5.csv')
stopwords = pd.read_json('./data/stopwords_ko.json')

# 중복 제거
df = df.drop_duplicates()

# date_time에 맞지 않는 데이터 삭제
df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
df['id'] = pd.to_numeric(df['id'], errors='coerce')
df['conversation_id'] = pd.to_numeric(df['conversation_id'], errors='coerce')

# 시간 조정
df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)
df['date'] = df['date_time'].dt.date

# tweet column 타입 string으로 변경 & 소문자로 변경
df['tweet'] = df.tweet.astype(str)
df['tweet'] = df['tweet'].apply(lambda x: x.lower())

# 단어별로 자른 것 넣을 새로운 column 만들기
df['word'] = ''

h = Hannanum()
sum = 0 

emoji_pattern = re.compile("["
                        u"\U0001F600-\U0001F64F"  # emoticons
                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                        u"\U00002702-\U000027B0"
                        u"\U00002600-\U000027BF"
                        "]+", flags=re.UNICODE)

hangul = re.compile("[^"
                u"\U0000AC00-\U0000D7AF"
                "]+", flags=re.UNICODE)

for row in df.itertuples():
    sum = sum + 1
    tweet = str(row.tweet).decode('utf-8', errors='replace').replace("＂", "").replace("［", "").replace("］","").replace("", "").replace("！", "").replace("？", "").replace("｀", "").replace("�", "").replace("＇", "").replace("🥰", "").replace("♥️", "")
    hangul_text = emoji_pattern.sub(r'', tweet)
    hangul_text = re.sub(hangul, ' ', hangul_text)
    print(str(hangul_text))
    print(sum)
    print("==========================")
    # spell_ok = spell_checker.check(str(hangul_text))
    # word_str = get_nouns(str(spell_ok.checked))
    if(hangul_text.isspace() == False):
        word_str = get_nouns(str(hangul_text))
        df.at[row.Index, 'word'] = word_str

# csv 저장
df[['date', 'word', 'username', 'tweet']].to_csv("./data/clean_data.csv", mode="w")

