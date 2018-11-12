#!/usr/bin/env python3
import pandas as pd
import datetime

def main():
    df = pd.read_csv('data/tweet_test.csv',
        # usecols=['id', 'conversation_id', 'date', 'time', 'username', 'name', 'tweet'],
        usecols=['date', 'time'],
        parse_dates=[['date', 'time']],
        # dayfirst=True
        # dtype={
        #     'id': pd.np.float64
        # },
        # encoding='utf-8'
        )
    df['date_time'] = pd.to_datetime(df['date_time'], errors='coerce')
    df['date_time'] = df['date_time'] - datetime.timedelta(hours=16)
    print(df.head())

if __name__ == "__main__":
    main()