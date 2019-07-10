import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.functions import col
import load_to_spark
import hotspot_detection

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
bots = ["Bot", "Bots"]

def init_df(filenames):
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    dfbots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(dfbots)
    return df

def draw_histogram(df1, df2):
    plt.rcParams.update({})
    fig, axes = plt.subplots()
    axes.set_yscale("log")
    axes.set_xlabel("Anzahl der Hotspots")
    axes.set_ylabel("Anzahl der Artikel")
    hist(axes, [df1, df2], bins=100, color=["red", "blue"])
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/hotspot_weekly_vs_monthly.png")

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

#filenames.append(base_path + "1.json")

df = init_df(filenames)
df_h_weekly = hotspot_detection.sliding_window_hotspots_by_time(df, window_size="1")
df_h_monthly = hotspot_detection.sliding_window_hotspots_by_time(df, window_size="4")

df_g1 = df_h_weekly.groupBy(col("title")).count()
df_g2 = df_h_monthly.groupBy(col("title")).count()

df_h1 = df_g1.select(col("count"))
df_h2 = df_g2.select(col("count"))

draw_histogram(df_h1, df_h2)
