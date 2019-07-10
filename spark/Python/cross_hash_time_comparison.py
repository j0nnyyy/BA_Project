from pyspark.sql.functions import col
import jaccard_similarity
import load_to_spark
import time
import argparse

base_path = "/scratch/wikipedia-dump/wiki_small_"
logpath = "/home/ubuntu/BA_Project/log/cross_hash_time_log.txt"
filenames = []
bots = ["Bot", "Bots"]
samples = 10000

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
parser.add_argument("--samples")
args = parser.parse_args()

if args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

if args.samples:
    samples = int(args.samples)

df = load_to_spark.main_init_df(filenames).select(col("author"), col("title"))
df = df.where(col("author").isNotNull())
df_bots = df.where(col("author").rlike("|".join(bots)))
df_authors = df.subtract(df_bots).distinct()

#select samples
count = df_authors.count()
df_sample = df_authors.sample(False, fraction=samples/count, seed=int(round(time.time())))
df_sample.cache()

start_cross = time.time()
df_tmp1 = jaccard_similarity.jaccard_with_crossjoin(df_sample, "title", "author", maxval=0.9)
print(df_tmp1.count())
end_cross = time.time()
start_hash = time.time()
df_tmp2 = jaccard_similarity.jaccard_with_min_hashing(df_sample, "title", "author", maxval=0.9)
print(df_tmp2.count())
end_hash = time.time()

file = open(logpath, "+a")
#workers, samples, crosstime, hashtime
output = "{} {} {} {}\n".format(6, samples, end_cross - start_cross, end_hash - start_hash)
file.write(output)
file.close()
print("Done")
