from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, desc, asc, to_timestamp, months_between, datediff
import pyspark.sql.functions as f
import load_to_spark
import matplotlib.pyplot as plt
from pyspark_dist_explore import Histogram, hist, distplot
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import datetime
import pyspark.sql.types

slen = udf(lambda s: len(s), IntegerType())


def months_from_creation(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
        .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
        .withColumn("date", df["revision"].getItem(0).timestamp)\
        .withColumn("creation_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))\
        #.withColumn("months_from_creation", f.round(months_between(current_timestamp(), col("creation_date")), -1))
    df_res = df.drop(*columns_to_drop)
    return df_res


def last_edit(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("date", df["revision"].getItem(slen(df.revision)-1).timestamp)\
        .withColumn("last_edit_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    df_res = df.drop(*columns_to_drop)
    return df_res



def join_df(df1, df2):
    df_res = df1.join(df2, "title")\
        .withColumn("active time",
                    f.round(months_between(col("last_edit_date"), col("creation_date")), -1)) \
        .withColumn("time_since_last_edit",
                    f.round(months_between(to_timestamp(f.lit("2018-05-21")), col("last_edit_date"))))
    #.drop("id")
    return df_res


def active_article_time(df):
    df_last_edit_date = last_edit(df)
    df_creation_date = months_from_creation(df)
    df_res = join_df(df_creation_date, df_last_edit_date)
    return df_res


def draw_histogram(df, histogram_name):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)

    hist(axes[0, 0], [df], bins=20, color=['red'])
    axes[0, 0].set_title(histogram_name)
    #axes[0, 0].legend()
    plt.savefig(histogram_name)


df = load_to_spark.init()
df_res = active_article_time(df)
df_res.show()

# Create active time histogram
df_active_time = df_res.select(col("active time").alias("active"))
df_active = df_active_time.orderBy(desc("active"))
df_active.cache()
df_active.persist()
df_active.show()

fig, axes = plt.subplots(nrows=2, ncols=2)
fig.set_size_inches(20, 20)


hist(axes[0, 0], [df_active], bins=20, color=['red'])
axes[0, 0].set_title('Active Time')
#draw_histogram(df_active, 'ActiveTime.pdf')

df_last_edit = df_res.select(col("time_since_last_edit").alias("since_last_edit"))
df_since_last_edit = df_last_edit.orderBy(desc("since_last_edit"))
df_since_last_edit.cache()
df_since_last_edit.persist()
df_since_last_edit.show()
#draw_histogram(df_since_last_edit, 'TimeSinceLastEdit.pdf')

hist(axes[0, 1], [df_since_last_edit], overlapping=True)
axes[0, 1].set_title('Time Since Last Edit')

plt.savefig('Histogram')
print('DONE')


def generate_date_series(start, stop):
    return [start + datetime.timedelta(days=x) for x in range(0, (stop-start).days + 1)]


#spark.udf.register("generate_date_series", generate_date_series, ArrayType(DateType()))
'''
w = Window.orderBy("yearmonth")
tempDf = df.withColumn("diff", months_between(f.lead(col("yearmonth"), 1).over(w), col("yearmonth")))\
    .filter(col("diff") > 1)\
    .withColumn("quantity", f.lit("0"))\
    .withColumn("missing_dates", explode(generate_date_series(col("yearmonth"), col("diff"))))\
    .withColumn("date", col("date").cast("timestamp"))
'''
winSpec = Window.partitionBy("title")
