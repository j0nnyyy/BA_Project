from pyspark.sql.functions import col, window, year, month
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

def draw_histogram(values):
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    plt.hist(values, 588)
    axes.set_xlabel("Zeit seit dem 01.01.1970 in Monaten")
    axes.set_ylabel("Anzahl der Werte")
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/global_hotspots.png")

df = init_df()
df_hotspots = df.groupBy(window(col("timestamp"), str(window_size) + " weeks")).count()
df_hotspots = df_hotspots.withColumn("years_since_epoch", year(col("window")["start"]) - 1970)
df_hotspots = df_hotspots.withColumn("months_since_epoch", (col("years_since_epoch") * 12) + month(col("window")["start"]))
rows = df_hotspots.select(col("count"), col("months_since_epoch")).collect()

values = []
for i in range(len(rows)):
    count = rows[i]["count"]
    months = rows[i]["months_since_epoch"]
    if months not in values:
        for j in range(count):
            values.append(months)

draw_histogram(values)
