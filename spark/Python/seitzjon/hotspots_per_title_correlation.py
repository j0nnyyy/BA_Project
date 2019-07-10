import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.functions import col
import load_to_spark
import jaccard_similarity
import hotspot_detection
import argparse

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
filenames = []
to_compare = "title"

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
parser.add_argument("--filecount")
parser.add_argument("--compare")
args = parser.parse_args()

if args.filecount:
    filecount = int(args.filecount)
    if filecount > 1:
        for i in range(1, filecount + 1):
            filenames.append(base_path + str(i) + ".json")
    elif args.filenumber:
        filenames.append(base_path + args.filenumber + ".json")
    else:
        filenames.append(base_path + "1.json")
elif args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

if args.compare:
    to_compare = args.compare

def init_df():
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(df_bots)
    return df

def init_category_df(df):
    df_categories = load_to_spark.create_category_df()
    df_joined = df.select("id", "title").distinct().join(df_categories, "id")
    return df_joined

def draw_histogram(df):
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    axes.set_xlabel("Anzahl der Hotspots")
    axes.set_ylabel("Anzahl der Titel")
    hist(axes, [df], bins=20, color=["red"])
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/hotspots_per_title_correlation_author.png")

df = init_df()
df_category = init_category_df(df)
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df, type=to_compare).select("window", "title")
df_hotspots.cache()
df_hotspots.show()

df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_hotspots, "title", "window")
#window1|window2
df_similar = df_jaccard.where(col("jaccard") < 0.3).select("title1", "title2")
df_titles = df_similar.select(col("title1").alias("title")).union(df_similar.select(col("title2").alias("title"))).distinct()
df_count = df_hotspots.join(df_titles, "title")
df_grouped = df_count.groupBy(col("title")).count()

df_hist = df_grouped.select(col("count"))
draw_histogram(df_hist)
