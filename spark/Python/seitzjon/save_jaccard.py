import jaccard_similarity
import load_to_spark
from pyspark.sql.functions import col

bots = ["Bot", "Bots"]

df = load_to_spark.main_init_df("/scratch/wikipedia-dump/wiki_small_11.json")
df = df.where(col("author").isNotNull())
dfbots = df.where(col("author").rlike("|".join(bots)))
df = df.subtract(dfbots)
df.show()

df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df, "title", "author")
load_to_spark.save_df(df_jaccard, "/scratch/wikipedia-dump/jaccard_dump")
