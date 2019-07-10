from pyspark.sql.functions import window, avg, col
from pyspark_dist_explore import hist
import matplotlib as plt
import load_to_spark
import argparse
import time

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []

for i in range(1, 26):
    filenames.append(base_path + str(i) + ".json")

def init_df():
    df = load_to_spark.init_article_hotspot_df(filenames)
    return df

def create_windows(df):
    df_windowed = df.groupBy(col("title"), window(col("timestamp"), "4 weeks"))\
        .count().groupBy(col("window")).avg(col("count"))
    return df_windowed

def draw_histogram(values, bins):
    plt.set_xlabel("Months since 1.1.1970")
    plt.set_ylabel("Average count of revisions per article")
    plt.hist(values, bins)
    plt.savefig("/scratch/wikipedia-dump/plots/average_revisions_per_month.png")

def window_in_months(df):
    df_months = df.withColumn("years", year(col("window")["start"]))
    df_months = df.withColumn("months", (col("years") * 12) + month(col("window")["start"]))
    df_months = df.select("months", col("avg(count)").alias("avg"))
    return df_months

df = init_df()
df_windowed = create_windows(df)
df_avg = window_in_months(df_windowed)
rows = df_avg.collect()

values = []
for i in range(len(rows)):
    count = int(rows[i]["avg"])
    for j in range(count):
        values.append(int(rows[i]["months"]))

draw_histogram(values, len(rows))
