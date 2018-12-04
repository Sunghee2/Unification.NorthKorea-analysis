# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
# from pyspark.ml.evaluation import RegressionEvaluator
# from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, split, to_date, col, regexp_replace, decode, row_number, encode, udf, when, lit, concat
from pyspark.sql.window import Window
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

spark = SparkSession.builder.appName("sentiment_analysis").getOrCreate()

tweets = spark.read.load("hdfs:///user/maria_dev/data/clean_data.csv",
                    format="csv", sep=",", inferSchema="true", header="true", encoding="utf-8")\
                    .select("date", "tweet", "username", "word")
sentiment_df = spark.read.load("hdfs:///user/maria_dev/data/sentiment_training_data/polarity.csv",
                    format="csv", sep=",", inferSchema="true", header="true")

# date별 언급량
tweets_num = tweets.groupBy("date").count().orderBy("date", ascending=0)
tweets_num = tweets_num.na.drop()

# 중복 제거
# tweets = tweets.dropDuplicates(['username', 'tweet'])

tweets = tweets.select(["word", "date"])
# tweets.show()

# tweets = tweets.withColumn('word', col(tweets.word).str.decode('utf-8'))

# flatten word
tweets = tweets.withColumn("word", explode(split("word", '\\|')))

tweets = tweets.filter(
    ~(tweets.word.contains("문재인")) & 
    ~(tweets.word.contains("대통령")) &
    ~(tweets.word.contains("문재"))
)

# parse date type
tweets = tweets.withColumn("date", to_date("date"))


# date별 word count
tweets = tweets.groupBy(["word", "date"]).count().orderBy(["date", "count"], ascending=[0, 0])

# 여기까지 확인함

# # date별 많은 word 15개 추출
w = Window().partitionBy("date").orderBy(col("count").desc())
tweets = tweets.withColumn("rn", row_number().over(w)).where(col("rn") <= 15)\
                .select("word", "date", "count")\
                .orderBy(["date", "rn"], ascending=[0,1])

# tweets.show(100)
# tweets_num.show(tweets_num.count(), False)

# 감정 사전
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
dict_VA = split_VA.toPandas().set_index("ngram")["max.value"].to_dict()

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

split_col = split(tweets.word, "\\,")
tweets = tweets.withColumn("split_word", split_col.getItem(0))
tweets = tweets.withColumn("pos", split_col.getItem(1))

udf_calc = udf(get_sentiment, StringType())
tweets = tweets.withColumn("sentiment", udf_calc(col("split_word"), col("pos")))



# tweets.select(explode(split('word', '\\|')).alias('word'), to_date('date')).show()
# tweets.createOrReplaceTempView("tweets")
# # training_data.createOrReplaceTempView("training_data")