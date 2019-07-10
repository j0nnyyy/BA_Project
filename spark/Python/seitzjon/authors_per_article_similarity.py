import load_to_spark
import jaccard_similarity
from pyspark.sql.functions import col, avg

bots = ["Bot", "Bots"]

df = load_to_spark.main_init_df("/scratch/wikipedia-dump/wiki_small_11.json").select("title", "author").distinct()
df = df.where(col("author").isNotNull())
dfbots = df.where(col("author").rlike("|".join(bots)))
df = df.subtract(dfbots)
df.cache()

df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df, "title", "author")
df_jaccard.cache()

df_similar = df_jaccard.where(col("jaccard") < 0.3).select("title1", "title2")
df_rest = df_jaccard.where((col("jaccard") >= 0.3) & (col("jaccard") <= 0.7)).select("title1", "title2")

df_sim_t = df_similar.select(col("title1")).union(df_similar.select(col("title2")))\
    .distinct()
df_sim_t.show()
df_rest_t = df_rest.select(col("title1")).union(df_rest.select(col("title2"))).distinct()
df_rest_t.show()

df_sim_joined = df.join(df_sim_t, col("title1") == col("title"))\
    .select("author", "title").distinct()
print("similar")
df_sim_joined.groupBy(col("title")).count()\
    .select(avg(col("count"))).show()

df_rest_joined = df.join(df_rest_t, col("title1") == col("title"))\
    .select("author", "title").distinct()
print("rest")
df_rest_joined.groupBy(col("title")).count()\
    .select(avg(col("count"))).show()

df_all_titles = df.select(col("title")).distinct()
df_jacc_titles = df_sim_t.union(df_rest_t).select(col("title1").alias("title"))
df_rest = df_all_titles.subtract(df_jacc_titles)
df_rest = df_rest.select(col("title").alias("title1"))
df_joined = df.join(df_rest, col("title") == col("title1")).select("author", "title").distinct()
print("different")
df_joined.groupBy(col("title")).count().select(avg(col("count"))).show()
