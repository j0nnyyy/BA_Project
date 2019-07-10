import load_to_spark
from pyspark.sql.functions import col, avg

filenames = []

#for i in range(1, 27):
#    path = "/scratch/wikipedia-dump/wiki_small_" + str(i) + ".json"
#    filenames.append(path)

path = "/scratch/wikipedia-dump/wiki_small_11.json"
filenames.append(path)
df = load_to_spark.main_init_df(filenames).select("title", "author").distinct()
print(df.count())
df_grouped = df.groupBy(col("title")).count()
df_avg = df_grouped.select(avg(col("count")))
df_avg.show()
