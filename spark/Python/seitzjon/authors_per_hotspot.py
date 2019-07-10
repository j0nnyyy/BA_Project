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

for i in range(1, 27):
    filenames.append(base_path + str(i) + ".json")

df = init_df(filenames)
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df)

df_authors = df.select("author", "timestamp", col("title").alias("title1"))
df_joined = df_hotspots.join(df_authors, (col("title") == col("title1")) & (col("timestamp").between(col("window")["start"], col("window")["end"])))\
    .select("title", "author", "window").distinct()

df_joined.groupBy("title", "window")
