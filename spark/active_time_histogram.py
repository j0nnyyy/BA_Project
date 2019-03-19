#changed matplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('Agg')

from pyspark.sql.functions import from_unixtime, col, desc, asc, to_timestamp, months_between, udf
import pyspark.sql.functions as f
import load_to_spark
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.types import IntegerType
import time

logpath = '/home/ubuntu/BA_Project/log.txt'

#retrieve loaded file count
file_count = load_to_spark.filename.count(',') + 1

slen = udf(lambda s: len(s), IntegerType())

def creation_date_of_article(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
        .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
        .withColumn("date", df["revision"].getItem(0).timestamp)\
        .withColumn("creation_date_of_article", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))\
        .withColumn("time_since_creation",
                    f.round(months_between(col("creation_date_of_article"), to_timestamp(f.lit("2001-01-15")))))
    df_res = df.drop(*columns_to_drop)
    return df_res


def last_edit_date(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("date", df["revision"].getItem(slen(df.revision)-1).timestamp)\
        .withColumn("last_edit_date", from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    df_res = df.drop(*columns_to_drop)
    return df_res


def join_df(df1, df2):
    df_joined = df1.join(df2, "title")\
        .withColumn("active time",
                    f.round(months_between(col("last_edit_date"), col("creation_date_of_article")), -1)) \
        .withColumn("time_since_last_edit",
                    f.round(months_between(to_timestamp(f.lit("2018-05-21")), col("last_edit_date"))))
    return df_joined


def join_last_and_creation_dates(df):
    df_last_edit_date = last_edit_date(df)
    df_creation_date = creation_date_of_article(df)
    df_res = join_df(df_creation_date, df_last_edit_date)
    return df_res


def draw_histogram(df1, df2, df3):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df1], bins=20, color=['red'])
    axes[0, 0].set_ylim([0, 50])
    axes[0, 0].set_xlim([0, 30])
    axes[0, 0].set_title('Aktualitaet des Artikels')
    axes[0, 0].set_xlabel('Zeitraum (Anazhl Monaten)')
    axes[0, 0].set_ylabel('Laenge (Anzahl Revisionen)')
    hist(axes[0, 1], [df2], bins=20, color=['blue'])
    axes[0, 1].set_title('Laenge der Artikel-Historie')
    axes[0, 1].set_xlabel('Zeitraum (Anazhl Monaten)')
    axes[0, 1].set_ylabel('Laenge (Anzahl Revisionen)')
    hist(axes[1, 0], [df3], bins=20, color=['purple'])
    axes[1, 0].set_title('Erstellungsdatum des Artikels')
    axes[1, 0].set_xlabel('Zeitraum (Anazhl Monaten)')
    axes[1, 0].set_ylabel('Anzahl Artikeln')
    plt.savefig('CreationTime_ActiveTime_TimeSinceLastEdit_Histogram')

#get start time
start_time = time.time()
	
df = load_to_spark.init()

#retrieve spark worker count
worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1

df_res = join_last_and_creation_dates(df)
df_res.show()

# Draw histograms
df_article_creation = df_res.select(col("time_since_creation")).orderBy(desc("time_since_creation"))
df_article_creation.cache()
df_article_creation.persist()
df_article_creation.show()

df_active_time = df_res.select(col("active time").alias("active")).orderBy(desc("active"))
df_active_time.cache()
df_active_time.persist()
df_active_time.show()

df_since_last_edit = df_res.select(col("time_since_last_edit").alias("since_last_edit")).orderBy(desc("since_last_edit"))
df_since_last_edit.cache()
df_since_last_edit.persist()
df_since_last_edit.show()

#get end time
end_time = time.time()

draw_histogram(df_active_time, df_since_last_edit, df_article_creation)

#calculate duration and write the application information to the log file
duration = end_time - start_time
file = open(logpath, 'a+')
file.write(worker_count + " " + file_count + " " + duration)

print('DONE')



