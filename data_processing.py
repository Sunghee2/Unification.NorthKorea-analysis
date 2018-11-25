# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from konlpy.tag import Twitter

if __name__ == "__main__":
    spark = SparkSession.builder.appName("data_prepocessing").getOrCreate()


    # tweets = spark.read.format('csv').options(header='true').load('hdfs:///user/maria_dev/data/tweet_test.csv')

    # tweets.printSchema()
    tweets = spark.read.load("hdfs:///user/maria_dev/data/tweet_test.csv",
                        format="csv", sep=",", inferSchema="false", header="true").select(
                        'date', 'time', 'tweet'
            )

    # tweets.createOrReplaceTempView("tweets")
    # date와 time merge/cast timestamp
    tweets = tweets.withColumn('datetime', sf.unix_timestamp(sf.concat(sf.col('date'), sf.lit(' '),sf.col('time')), "yyyy.MM.dd HH:mm:ss").cast("timestamp"))
    # 시간 조정
    tweets = tweets.withColumn('datetime', (sf.unix_timestamp("datetime") - 57600).cast("timestamp"))
    # 중복 제거
    tweets = tweets.distinct()

    #57600
    tweets.show()
    tweets.printSchema()

    # result = spark.sql("""
    #     SELECT *
    #     FROM tweets
    #     LIMIT 10
    # """)

    # for row in result.collect():
    #     print(row.username, row.tweet, row.date, row.time)

# from pyspark import SparkConf, SparkContext
# from itertools import islice
# import csv

# if __name__ == "__main__":
#     conf = SparkConf().setAppName("data_processing")
#     sc = SparkContext(conf = conf)



    # lines = sc.textFile("hdfs:///user/maria_dev/data/tweet_test.csv", use_unicode=True)

    # counts = lines.flatMap(lambda line: str(line.encode('utf-8')).split("\n"))\
    #         .map(lambda line: (line.split("\t")[0], 1))\
    #         .reduceByKey(lambda a, b: a + b)


    # results = counts.take(5)

    # for line in lines:
    #     print(line)

    # for result in results:
    #     print(result)

