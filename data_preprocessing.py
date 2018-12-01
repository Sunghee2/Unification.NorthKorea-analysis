#-*- coding: utf-8 -*-
import pandas as pd
import datetime
import numpy as np
import re
from hanspell import spell_checker
from konlpy.tag import Hannanum
from konlpy.tag import Okt
# from konlpy.tag import Komoran
# from konlpy.tag import Twitter
from collections import Counter
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
    nouns = [item for item in pos if item[1] == 'NC' or item[1] == 'NQ' or item[1] == 'NN']
    split_nouns = "|".join("%s" % tup[0] for tup in nouns)
    for stopword in stopwords.itertuples(): # 불용어 체크
        if dct.get(stopword._1):
            del dct[stopword._1]
    return split_nouns

df = read_data('./data/tweet_test.csv')
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

h = Hannanum()
hangul = re.compile('[^ ㄱ-ㅣ가-힣]+')
for row in df.head(20).itertuples():
    hangul_text = hangul.sub("",row.tweet).replace("‘", "").replace("’", "").replace("“", "").replace("”", "")
    # URLless_string = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', hangul_text)
    spell_ok = spell_checker.check(str(hangul_text))
    word_str = get_nouns(str(spell_ok.checked))
    df.at[row.Index, 'word'] = word_str

# csv 저장
# df[['date', 'word', 'username', 'tweet']].to_csv("./data/clean_data.csv", mode="w")