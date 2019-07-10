from pyspark.sql import Window
import load_to_spark
import hotspot_detection
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.functions import col
import argparse

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
filenames = []
type = "title"

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

#filenames.append(base_path + "1.json")

parser = argparse.ArgumentParser()
parser.add_argument("--type")
args = parser.parse_args()

if args.type:
    type = args.type

def init_df():
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(df_bots)
    return df

def draw_histogram(df):
    plt.yscale('log')
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    axes.set_yscale("log")
    axes.set_xlabel("Anzahl der Hotspots")
    if type == "title":
    	axes.set_ylabel("Anzahl der Titel")
    elif type == "author":
        axes.set_ylabel("Anzahl der Autoren")
    hist(axes, [df], bins=20, color=["red"])
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/" + type + "_hotspot_count.png")

print("loading files")
df = init_df()
print("calculating hotspots")
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df, type=type)
print("counting hotspots")
df_grouped = df_hotspots.groupBy(col(type)).count()
print("selecting needed columns")
df_hist = df_grouped.select("count")
print("drawing histogram")
draw_histogram(df_hist)
