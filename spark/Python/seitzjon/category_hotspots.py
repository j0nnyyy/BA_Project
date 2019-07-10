from pyspark.sql.functions import concat, col, lit
import hotspot_detection
import load_to_spark
import jaccard_similarity
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []

def draw_histogram(df):
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes, [df], bins=20, color=['red'])
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/article_category_hotspot_jaccard.png")

#for i in range(1, 6):
#    filenames.append(base_path + str(i) + ".json")

filenames.append(base_path + "11.json")

#revID|author|timestamp|title
df_hot = load_to_spark.init_article_hotspot_df(filenames)
#title|window|rev_count
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df_hot)

#id|category
df_categories = load_to_spark.create_category_df("/scratch/wikipedia-dump/categorylinks.json")
#id|title|author|authorID|editTime
df = load_to_spark.main_init_df(filenames)
df = df.select("title", "author", col("editTime").alias("timestamp"), col("id").alias("id1")).distinct()

#title|author|category|timestamp
df_joined = df.join(df_categories, col("id") == col("id1")).drop("id1", "id")
df_joined.count()
df_joined.distinct().count()

#category|window|rev_count
df_category_hotspots = hotspot_detection.sliding_window_category_hotspots(df_joined)

#put '::C::' in front of each category title to prevent overlaps between category
#titles and article titles
df_category_hotspots = df_category_hotspots.withColumn("category", concat(lit("::C::"), col("category")))

df1 = df_hotspots.select(col("title").alias("to_compare"), col("window"))
df2 = df_category_hotspots.select(col("category").alias("to_compare"), col("window"))
print("article", df1.count())
df1.show()
print("category", df2.count())
df2.show()

df_jaccard = jaccard_similarity.jaccard_two_dfs(df1, df2, "to_compare", "window")
df_jaccard.show()

df_hist = df_jaccard.select("jaccard")
draw_histogram(df_hist)
