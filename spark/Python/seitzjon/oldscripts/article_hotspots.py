from pyspark.sql.functions import col, window
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import hotspot_detection
import load_to_spark
import argparse

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
window_size = "2"
multiplier = 2
filenames = []

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
parser.add_argument("--filecount")
parser.add_argument("--windowsize", help="the window size in weeks")
parser.add_argument("--multiplier", help="the multiplier to identify hotspots")
parser.add_argument("--articlecount", help="single or multiple")
args = parser.parse_args()

if args.filecount:
    if args.filecount > 1:
        for i in range(1, int(args.filecount) + 1):
            filenames.append(base_path + str(i) + ".json")
    else:
        filenames.append(base_path + "1.json")
elif args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

if args.windowsize:
    window_size = args.windowsize

if args.multiplier:
    multiplier = int(args.multiplier)

def init_df():
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(df_bots)
    print(df.count())
    return df

def df_with_authors(df1, df2):
    df1 = df1.select("author", "timestamp", col("title").alias("title1"))
    df2 = df2.select("window", "count", "avg(count)", col("title").alias("title2"))
    df_joined = df1.join(df2, (col("title1") == col("title2")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
        .select(col("title1").alias("title"), col("author"), col("count").alias("rev_count"), col("window")).distinct()
    return df_joined

def author_percentage_per_window(df):
    df_grouped = df.groupBy(col("title"), col("rev_count"), col("window")).count()\
        .select(col("title"), col("rev_count"), col("window"), col("count").alias("author_count"))
    df_percentage = df_grouped.withColumn("percentage", col("author_count") / col("rev_count"))
    return df_percentage

def avg_author_percentage_per_article_hotspot(df):
    df_grouped = df.groupBy("title").avg("percentage")
    return df_grouped

def draw_histogram(df):
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes, [df], bins=20, color=['red'])
    axes.set_xlabel("Durchschnittliches Verhältnis von Autoren zu Revisionen in 4 Wöchigen Zeiträumen")
    axes.set_ylabel("Anzahl der Werte")
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/hotspot_author_percentage.png")

df = init_df()
df_hotspots = hotspot_detection.single_revision_hotspots_by_time(df, weeks_per_bin=4, multiplier=4)
df_hotspots.show()
print(df_hotspots.count())

df_authors = df_with_authors(df, df_hotspots)
df_a_per_window = author_percentage_per_window(df_authors)
df_a_per_window.show()

df_avg_percentage = avg_author_percentage_per_article_hotspot(df_a_per_window)
df_avg_percentage.show()

df_hist = df_avg_percentage.select(col("avg(percentage)"))
draw_histogram(df_hist)
