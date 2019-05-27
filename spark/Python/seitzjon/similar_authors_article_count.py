from pyspark.sql.functions import col, avg
import jaccard_similarity
import load_to_spark
import argparse
import time

base_path = "/scratch/wikipedia-dump/wiki_small_"
log_path = "/home/ubuntu/BA_Project/log/similar_author_article_count_log.txt"
bots = ["Bot", "Bots"]
filenames = []

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
args = parser.parse_args()

if args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

df = load_to_spark.main_init_df(filenames)
df_authors = df.select(col("author"), col("title")).where(col("author").isNotNull())
df_bots = df_authors.where(col("author").rlike("|".join(bots)))
df_authors = df_authors.subtract(df_bots).distinct()

#select samples
count = df_authors.count()
print("selecting samples")
df_samples = df_authors.sample(False, fraction=20000.0 / count, seed=int(round(time.time())))

df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_samples, "author", "title", maxval=0.3)
df_joined = df_authors.join(df_jaccard, (col("author") == col("author1")) | (col("author") == col("author2")))\
    .select(col("author"), col("title")).distinct()
df_counts = df_joined.groupBy(col("author")).count()
df_avg = df_counts.select(avg(col("count")))
avg = float(df_avg.first()["avg(count)"])

file = open(log_path, "+a")
output = "{} {}\n".format(args.filenumber, avg)
file.write(output)
file.close()
print("Done")
