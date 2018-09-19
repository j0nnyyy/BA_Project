from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, desc, to_timestamp, months_between, current_timestamp, percent_rank
import pyspark.sql.functions as func
from pyspark.sql.window import Window
import load_to_spark
import number_of_revisions_per_article


#df.filter(df['count'] > 5).agg({"age": "avg"})



def months_from_creation(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
        .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
        .withColumn("date", df["revision"].getItem(0).timestamp)\
        .withColumn("creation_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))\
        .withColumn("months_from_creation", func.round(months_between(current_timestamp(), col("creation_date")), -1))
    df_res = df.drop(*columns_to_drop)
    return df_res


def average_stats(df1, df2):
    df_avg = df1.join(df2, "title")\
        .withColumn("average edit history", func.round((col("months_from_creation") / col("edit history length")), 0))
        #.drop("months_from_creation")
    return df_avg


#def median_stats(df):
    #return df_median

def percentiles_of_article(df):
    w = Window.orderBy(col("title"))
    df_res = df.select("title", percent_rank().over(w).alias("percentile")).where('percentile == 0.6')
    return df_res

df = load_to_spark.init()
df1 = months_from_creation(df)
df2 = number_of_revisions_per_article.revisions_per_article()


dv_avg = average_stats(df1, df2)
dv_avg.cache()
dv_avg.persist
dv_avg.show(df.count(), False)
