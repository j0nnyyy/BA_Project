import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql.functions import col
import load_to_spark
import hotspot_detection

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
filenames = []

def init_dataframes(filenames):
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    dfbots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(dfbots)
    df_category = load_to_spark.create_category_df()
    df_t_id = df.select("title", "id")
    df_category = df_category.join(df_t_id, "id").select("title", "category")
    return df, df_category

def draw_histogram(df):
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    axes.set_yscale("log")
    axes.set_xscale("log")
    axes.set_xlabel("Anzahl der Hotspots")
    axes.set_ylabel("Anzahl der Kategorien")
    hist(axes, [df], bins=20, color=["red"])
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/category_hotspot_count.png")

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

#filenames.append(base_path + "1.json")

df, df_category = init_dataframes(filenames)
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df)
#title|window|rev_count|category
df_joined = df_category.join(df_hotspots, "title")
df_joined.orderBy(col("category").desc()).show()
print(df_joined.count())
df_grouped = df_joined.groupBy(col("category")).count()
df_grouped.show()

df_hist = df_grouped.select(col("count"))
df_hist.show()
draw_histogram(df_hist)
