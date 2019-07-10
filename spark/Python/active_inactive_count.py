from pyspark.sql.functions import col
import load_to_spark

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
bots = ["Bot", "Bots"]

for i in range(1, 27):
    path = base_path + str(i) + ".json"
    filenames.append(path)

print("Selecting real users")
df = load_to_spark.main_init_df(filenames).select(col("author"), col("title"))
df = df.where(col("author").isNotNull())
df_bots = df.where(col("author").rlike("|".join(bots)))
df_authors = df.subtract(df_bots).distinct()
df_authors.cache()
print("counting active and inactive")
df_authors = df_authors.groupBy(col("author")).count()
df_active = df_authors.where(col("count") > 10)
df_inactive = df_authors.subtract(df_active)

active = df_active.count()
inactive = df_inactive.count()
output = "active: " + str(active) + ", inactive: " + str(inactive)

file = open("/home/ubuntu/BA_Project/log/active_inactive_comparison.txt", "+w")
file.write(output)
file.close()
print("Done")
