from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, desc, asc, to_timestamp, months_between, current_timestamp, percent_rank, size
import pyspark.sql.functions as func
from pyspark.sql.window import Window
import load_to_spark
import number_of_revisions_per_article
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import datetime

from pyspark_dist_explore import Histogram, hist, distplot
from pyspark.sql import Row

slen = udf(lambda s: len(s), IntegerType())
LAST_EDIT_DATE = datetime.datetime.strptime("2018-05-21", "yyyy-MM-dd")


def calculate_window_expression(df):
    windowSpec = Window.partitionBy("title").orderBy("yearmonth").rowsBetween(-1, 1)
    df = df.withColumn("avg", func.avg("average edit history").over(windowSpec))
    return df


def months_from_creation(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
        .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
        .withColumn("date", df["revision"].getItem(0).timestamp)\
        .withColumn("creation_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))\
        .withColumn("months_from_creation", func.round(months_between(current_timestamp(), col("creation_date")), -1))
    df_res = df.drop(*columns_to_drop)
    return df_res


def last_edit(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("date", df["revision"].getItem(slen(df.revision)-1).timestamp)\
        .withColumn("last_edit_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
    df_res = df.drop(*columns_to_drop)
    return df_res


def join_df(df1, df2):
    df_res = df1.join(df2, "title")\
        .withColumn("active time",
                    func.round(months_between(col("last_edit_date"), col("creation_date")), -1))\
        .drop("id")
    return df_res


def average_stats(df1, df2):
    df_avg = df1.join(df2, "title")\
        .withColumn("average edit history", func.round((col("edit history length") / col("active time")), 2))
        #.drop("months_from_creation")
    return df_avg


def active_article_time(df):
    df_last_edit_date = last_edit(df)
    df_creation_date = months_from_creation(df)
    df_res = join_df(df_creation_date, df_last_edit_date)
    return df_res


def percentiles_of_article(df):
    w = Window.orderBy(col("title"))
    df_res = df.select("title", percent_rank().over(w).alias("percentile")).where('percentile == 0.6')
    return df_res


def percentage(df):
    df = df.groupBy("title").agg(sum("author").alias("author_count"))


def draw_histogram(df, histogram_name):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)

    hist(axes[0, 0], [df], bins=20, color=['red'])
    axes[0, 0].set_title(histogram_name)
    axes[0, 0].legend()
    plt.savefig(histogram_name)


df = load_to_spark.init()
df_res = active_article_time(df)
#df_res.show()

# Create active time histogram
df_active_time = df_res.select(col("active time").alias("active"))
df_active = df_active_time.orderBy(asc("active"))
df_active.cache()
df_active.persist()
df_active.show()
draw_histogram(df_active, 'Active time histogram.pdf')

df2 = number_of_revisions_per_article.revisions_per_article()
df_avg = average_stats(df_res, df2)
df_avg.cache()
df_avg.persist()
df_avg.show()

#df_windexpr = calculate_window_expression(df_avg)
#df_windexpr.show()

#dv_avg.show(df.count(), False)
