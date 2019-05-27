from pyspark.sql.functions import col, abs, avg, asc
from pyspark.sql import Row
import jaccard_similarity
import load_to_spark
import time
import argparse

bot_names = ["Bot", "Bots"]
base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
samples = 10000

parser = argparse.ArgumentParser()
parser.add_argument("--filter", help="filter for what to compare")
parser.add_argument("--samples", help="number of samples to take")
parser.add_argument("--filenumber")
args = parser.parse_args()

if args.filter:
    filter = args.filter

if args.samples:
    samples = float(args.samples)

if args.filenumber:
    output = args.filenumber + " "
    path = base_path + args.filenumber + ".json"
    filenames.append(path)
else:
    output = "1 "
    filenames.append('/scratch/wikipedia-dump/wiki_small_1.json')

df = load_to_spark.main_init_df(filenames)

print("Selecting real users")
df_t_a = df.select(df.title, df.author)
df_t_a = df_t_a.where(col("author").isNotNull())
df_t_bot = df_t_a.where(df_t_a.author.rlike('|'.join(bot_names)))
df_t_user = df_t_a.subtract(df_t_bot)
df_t_user = df_t_user.distinct()
df_t_user.show() #show 1
print("Real Users Selected")

print("getting samples")
count = df_t_user.count()
df_t_user = df_t_user.sample(False, fraction=samples / count, seed = int(round(time.time() * 1000)))
df_t_user.cache()
print(df_t_user.count())

df1 = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "title", "author", maxval=1.0).select(col("title1").alias("t11"), col("title2").alias("t12"), col("jaccard").alias("jaccard1"))
df1 = df1.where((col("jaccard1") != 1.0))
df1.cache()
df1.orderBy(asc("jaccard1")).show()
df1.count()
print("DF1", df1.count())
df2 = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "title", "author", maxval=1.0).select(col("title1").alias("t21"), col("title2").alias("t22"), col("jaccard").alias("jaccard2"))
df2.cache()
df2.orderBy(asc("jaccard2")).show()
print("DF2", df2.count())

if filter == "only_non_similar":
    df1 = df1.where(col("jaccard1") > 0.6)
    df2 = df2.where(col("jaccard2") > 0.6)
elif filter == "only_similar":
    df1 = df1.where(col("jaccard1") < 0.3)
    df2 = df2.where(col("jaccard2") < 0.3)

print("DF1:", df1.count())
print("DF2:", df2.count())

df1.orderBy(asc("t11"), asc("t12")).show()
df2.orderBy(asc("t21"), asc("t22")).show()

df_joined = df1.join(df2, ((col("t11") == col("t21")) & (col("t12") == col("t22"))) | ((col("t11") == col("t22")) & (col("t12") == col("t21"))))
df_joined = df_joined.select(col("t11").alias("title1"), col("t12").alias("title2"), col("jaccard1"), col("jaccard2"))
df_joined.repartition(2000)
df_joined.cache()
print(df_joined.count())

df_diff_cols = df_joined.where(col("jaccard1") != col("jaccard2"))
print("different values:", df_diff_cols.count())

df_different = df_joined.withColumn("difference", abs(col("jaccard1") - col("jaccard2")))
print("all values:", df_different.count())

df_avg_diff = df_different.select(avg(col("difference")))
df_avg_diff.show()

avg_diff = float(df_avg_diff.first()["avg(difference)"])
output = output + str(avg_diff) + "\n"
file = open("/home/ubuntu/BA_Project/log/cross_hash_avg_diff", "+a")
file.write(output)
file.close()
