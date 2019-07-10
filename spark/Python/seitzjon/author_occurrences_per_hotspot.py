import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.functions import col
import hotspot_detection
import load_to_spark

bots = ["Bot", "Bots", "bot", "bots"]
base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []

def draw_histogram(df, xaxistitle, yaxistitle, path, logscale=False):
    if logscale:
        plt.yscale('log')
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    axes.set_yscale("log")
    axes.set_xlabel(xaxistitle)
    axes.set_ylabel(yaxistitle)
    hist(axes, [df], bins=200, color=["red"])
    plt.savefig(path)

def different_articles_per_author(df):
    #all articles that an author has contributed in a hotspot
    df_different = df.select("author", "title").distinct()
    df_grouped = df_different.groupBy(col("author")).count().where(col("count") < 50000)
    #df_grouped = df_different.groupBy(col("author")).count().where(col("count") > 40000)
    df_grouped.show()
    draw_histogram(df_grouped.select(col("count")), "Anzahl der Artikel", "Anzahl der Autoren", "/scratch/wikipedia-dump/plots/hotspots/different_article_author_hotspots.png", True)

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

#load hotspot df
df_hot = load_to_spark.init_article_hotspot_df(filenames)
df_hot = df_hot.where(col("author").isNotNull())
df_h_bots = df_hot.where(col("author").rlike("|".join(bots)))
df_hot = df_hot.subtract(df_h_bots)

#load main df
df = load_to_spark.main_init_df(filenames).select("author", "title", col("editTime").alias("timestamp"))
df = df.where(col("author").isNotNull())
df_bots = df.where(col("author").rlike("|".join(bots)))
df = df.subtract(df_bots)
df.show()

df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df_hot).select("window", col("title").alias("title1"))
df_hotspots.show()
df_joined = df.join(df_hotspots, (col("title") == col("title1")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
    .select("author", "title", "window").distinct()

df_grouped = df_joined.groupBy(col("author")).count()
df_hist = df_grouped.select(col("count"))
df_hist = df_hist.where(col("count") < 10000)
draw_histogram(df_hist, "Vorkommen in Artikelhotspots", "Anzahl der Autoren", "/scratch/wikipedia-dump/plots/hotspots/author_occurrences_per_hotspot.png")

different_articles_per_author(df_joined)
