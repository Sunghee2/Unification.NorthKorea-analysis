# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
# from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import explode, split, to_date, col, regexp_replace, decode, row_number, encode
from pyspark.sql.window import Window
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession.builder.appName("sentiment_analysis").getOrCreate()

# tweets = spark.read.load("hdfs:///user/maria_dev/data/tweet_test.csv",
tweets = spark.read.load("hdfs:///user/maria_dev/data/result.csv",
                    format="csv", sep=",", inferSchema="true", header="true", encoding="utf-8")\
                    .select("word", "date", "tweet", "username")
# training_data = spark.read.load("hdfs:///user/maria_dev/data/sentiment_training_data/subjectivity-polarity.csv",
#                     format="csv", sep=",", inferSchema="true", header="true")

# date별 언급량
tweets_num = tweets.groupBy('date').count().orderBy("date", ascending=0)
tweets_num = tweets_num.na.drop()

# 중복 제거
tweets = tweets.dropDuplicates(['username', 'tweet'])

# tweets = tweets.withColumn('word', col(tweets.word).str.decode('utf-8'))

# tweets.show()

# flatten word
tweets = tweets.withColumn("word", explode(split("word", '\\|')))

tweets = tweets.where((col("word") != "문재인") & (col("word") != "대통령"))

# parse date type
tweets = tweets.withColumn("date", to_date("date"))

# date별 word count
tweets = tweets.groupBy(['word', 'date']).count().orderBy(["date", "count"], ascending=[0, 0])

# date별 많은 word 15개 추출
w = Window().partitionBy('date').orderBy(col('count').desc())
tweets = tweets.withColumn('rn', row_number().over(w)).where(col('rn') <= 15)\
                .select('word', 'date', 'count')\
                .orderBy(['date', 'rn'], ascending=[0,1])

# tweets.show(100)
# tweets_num.show(tweets_num.count(), False)


# tweets.select(explode(split('word', '\\|')).alias('word'), to_date('date')).show()
# tweets.createOrReplaceTempView("tweets")
# # training_data.createOrReplaceTempView("training_data")
