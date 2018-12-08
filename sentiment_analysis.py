# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, split, to_date, col, regexp_replace, decode, row_number, encode, udf, when, lit, concat, sum
from pyspark.sql.window import Window
from starbase import Connection
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

# hbase 연동
c = Connection()
twitter = c.table("twitter")
if (twitter.exists()):
    twitter.drop()
twitter.create("moon", "unification", "dprk")
batch = twitter.batch()

def analysis(folder_name):
    tweets = spark.read.load("hdfs:///user/maria_dev/project/data/" + folder_name + "/clean_data.csv",
                    format="csv", sep=",", inferSchema="true", header="true", encoding="utf-8")

    # parse date type
    tweets = tweets.withColumn("date", to_date("date"))
    
    # date별 언급량
    tweets_num = tweets.groupBy("date").count().orderBy("date", ascending=0)
    tweets_num = tweets_num.na.drop()

    # flatten word
    tweets = tweets.withColumn("word", explode(split("word", '\\|')))

    # date별 word count
    tweets = tweets.groupBy(["word", "date"]).count().orderBy(["date", "count"], ascending=[0, 0])

    # word와 pos 나누기
    split_col = split(tweets.word, "\\,")
    tweets = tweets.withColumn("split_word", split_col.getItem(0))
    tweets = tweets.withColumn("pos", split_col.getItem(1))

    # 긍정 부정 구하기
    udf_calc = udf(get_sentiment, StringType())
    tweets = tweets.withColumn("sentiment", udf_calc(col("split_word"), col("pos")))

    # 긍부정 퍼센트 계산
    pos_percentage = tweets.groupBy(["date", "sentiment"]).sum().withColumnRenamed("sum(count)", "count")\
                        .withColumn("total", sum("count").over(Window.partitionBy("date")))\
                        .withColumn("percent", (col("count") / col("total")) * 100)\
                        .filter(col("sentiment").isin(["POS", "NEG"]))\
                        .select(["date", "sentiment", "percent"])\
                        .orderBy(["date", "percent"], ascending=[0,0])

    # # date별 많은 word 15개 추출
    w = Window().partitionBy("date").orderBy(col("count").desc())
    tweets = tweets.withColumn("rn", row_number().over(w)).where(col("rn") <= 15)\
                    .select("split_word", "date", "count", "pos", "sentiment")\
                    .orderBy(["date", "rn"], ascending=[0,1])

    # 용언에 '-다' 추가
    tweets = tweets.withColumn("split_word",\
                    when((tweets.pos == 'PV') | (tweets.pos == 'PA'), concat(col("split_word"), lit("다"))).otherwise(tweets.split_word))

    # nlp 안된 것 처리('김정'은)
    tweets = tweets.withColumn("split_word",\
                    when(tweets.split_word == '김정', concat(col("split_word"), lit("은"))).otherwise(tweets.split_word))

    import_data(folder_name, tweets, pos_percentage, tweets_num)

# 감정 사전
def make_sentiment_dict():
    global dict_NNG, dict_VV, dict_NNP, dict_VA

    sentiment_df = spark.read.load("hdfs:///user/maria_dev/polarity.csv",
                    format="csv", sep=",", inferSchema="true", header="true")

    sentiment_df = sentiment_df.filter(~sentiment_df.ngram.contains(";"))

    filter_NNG = sentiment_df.filter(sentiment_df.ngram.contains("NNG"))
    split_NNG = filter_NNG.withColumn("ngram", split(filter_NNG["ngram"], "/").getItem(0))
    split_NNG = split_NNG.withColumn("ngram", regexp_replace("ngram", "\*", ""))
    dict_NNG = split_NNG.toPandas().set_index("ngram")["max.value"].to_dict()

    filter_VV = sentiment_df.filter(sentiment_df.ngram.contains("VV"))
    split_VV = filter_VV.withColumn("ngram", split(filter_VV["ngram"], "/").getItem(0))
    split_VV = split_VV.withColumn("ngram", regexp_replace("ngram", "\*", ""))
    dict_VV = split_VV.toPandas().set_index("ngram")["max.value"].to_dict()

    filter_NNP = sentiment_df.filter(sentiment_df.ngram.contains("NNP"))
    split_NNP = filter_NNP.withColumn("ngram", split(filter_NNP["ngram"], "/").getItem(0))
    split_NNP = split_NNP.withColumn("ngram", regexp_replace("ngram", "\*", ""))
    dict_NNP = split_NNP.toPandas().set_index("ngram")["max.value"].to_dict()

    filter_VA = sentiment_df.filter(sentiment_df.ngram.contains("VA"))
    split_VA = filter_VA.withColumn("ngram", split(filter_VA["ngram"], "/").getItem(0))
    split_VA = split_VA.withColumn("ngram", regexp_replace("ngram", "\*", ""))
    dict_VA= split_VA.toPandas().set_index("ngram")["max.value"].to_dict()

def get_sentiment(word, pos):
        try:
            if(pos == "NC"):
                return dict_NNG[word.encode("utf-8").decode("utf-8")]
            elif(pos == "NQ"):
                return dict_NNP[word.encode("utf-8").decode("utf-8")]
            elif(pos == "PV"):
                return dict_VV[word.encode("utf-8").decode("utf-8")]
            elif(pos == "PA"):
                return dict_VA[word.encode("utf-8").decode("utf-8")]
            else:
                return None
        except KeyError, e:
            return None

def import_data(folder_name, tweets, pos_percentage, tweets_num):
    if batch:
        index = 1
        for row in tweets.rdd.collect():
            batch.insert(str(row["date"]), { folder_name : { "word%s" % index : row["split_word"] }}) 
            batch.insert(str(row["date"]), { folder_name : { "sentiment%s" % index : row["sentiment"] }}) 
            batch.insert(str(row["date"]), { folder_name : { "count%s" % index : row["count"] }}) 
            index = index + 1 if (index < 15) and (str(row["date"]) == prev_date) else 1
            prev_date = str(row["date"])

        for row in pos_percentage.rdd.collect():
            batch.insert(str(row["date"]), { folder_name : { row["sentiment"] : row["percent"] }}) 

        for row in tweets_num.rdd.collect():
            batch.insert(str(row["date"]), { folder_name : { "all" : row["count"] }}) 

        batch.commit(finalize=True)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("sentiment_analysis").getOrCreate()
    make_sentiment_dict()

    analysis("moon")
    # analysis("unification")
    # analysis("dprk")
