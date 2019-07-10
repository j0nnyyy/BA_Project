from pyspark.sql import Window
from pyspark.sql.functions import col, window, lead, lag, when, avg, lit
import pyspark.sql.functions as f
import load_to_spark
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import hotspot_detection

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []

def draw_hist(df, xaxistitle, yaxistitle, path, logscale=False):
    if logscale:
        plt.yscale('log')
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes, [df], bins=20, color=['red'])
    axes.set_xlabel(xaxistitle)
    axes.set_ylabel(yaxistitle)
    plt.savefig(path)

def author_revisions_per_hotspot(df):
    df_filtered = df.select("author", "title", "window")
    df_grouped = df_filtered.groupBy("author", "window").count()
    df_grouped.where(col("count") > 20000).show()
    draw_hist(df_grouped.select(col("count")), "Anzahl der Revisionen", "Anzahl der Autoren", "/scratch/wikipedia_dump/plots/hotspots/hotspot_revisions_per_author.png")

def revisions_per_author(df_ratio, df_author):
    df_small = df_ratio.where(col("ratio") < 0.1)
    df_joined = df_small.join(df_author, (col("title") == col("title1")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
        .select("author", "window", "title", "timestamp")
    df_grouped = df_joined.groupBy("author", "window", "title").count()
    df_grouped.select(avg(col("count"))).show()

def authors_per_hotspot(df_ratio, df_author):
    df_small = df_ratio.where(col("ratio") < 0.1)
    df_joined = df_small.join(df_author, (col("title") == col("title1")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
        .select("author", "window", "title", "timestamp")
    df_grouped = df_joined.groupBy("title", "window").count()
    df_grouped.select(avg(col("count"))).show()

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

print("loading files")
df = load_to_spark.init_article_hotspot_df(filenames)

print("calculate hotspots")
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df)

df_author = df.select("author", "timestamp", col("title").alias("title1"))

#contains only hotspots
#title|window|author|timestamp|
df_joined = df_hotspots.join(df_author, (col("title") == col("title1")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
    .select("title", "author", "window", "rev_count")

df_joined = df_joined.distinct()
#count = number of authors at a titles hotspot
df_grouped = df_joined.groupBy(col("title"), col("window"), col("rev_count")).count()
#ratio between authors and revisions for a title in a hotspot
df_ratio = df_grouped.withColumn("ratio", col("count") / col("rev_count"))
df_ratio.show()

authors_per_hotspot(df_ratio, df_author)

revisions_per_author(df_ratio, df_author)

df_hist = df_ratio.select(col("ratio"))
draw_hist(df_hist, "Verhältnis von Autoren zur Anzahl der Revisionen in Hotpots", "Anzahl der Werte", "/scratch/wikipedia-dump/plots/hotspots/hotspot_author_ratio_sliding.png")

author_revision_per_hotspot(df_joined)
