from pyspark.sql.functions import avg, col
import jaccard_similarity
import load_to_spark
import argparse
import time

bots = ["Bot", "Bots"]
base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
filenumber = 1

parser = argparse.ArgumentParser()
parser.add_argument("--filecount")
parser.add_argument("--filenumber")
args = parser.parse_args()

if args.filecount:
    filecount = int(args.filecount)
    if filecount > 1:
        for i in range(1, filecount + 1):
            filenames.append(base_path + str(i) + ".json")
    else:
        filenames.append(base_path + "1.json")
elif args.filenumber:
    filenumber = int(args.filenumber)
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

def init_df():
    df = load_to_spark.main_init_df(filenames)
    #df = df.sample(False, fraction=1000000.0 / df.count(), seed=int(round(time.time())))
    df.cache()
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(df_bots)
    return df

df = init_df()
df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df, "author", "title", maxval = 0.8)
df_jaccard.cache()
print("Calculating similar avg")
df_similar = df_jaccard.where(col("jaccard") < 0.3)
df_authors = df_similar.select(col("author1"))\
    .union(df_similar.select(col("author2").alias("author1"))).distinct()
df_joined = df.join(df_authors, col("author") == col("author1"))\
    .select(col("author"), col("title")).distinct()
df_joined.groupBy(col("author")).count()\
    .select(avg(col("count"))).show()
#print("Calculating different avg")
#df_different = df_jaccard.where(col("jaccard") > 0.7)
#df_joined = df.join(df_different, (col("author") == col("author1")) | (col("author") == col("author2")))\
#    .select(col("author"), col("title")).distinct()
#df_joined.groupBy(col("author")).count()\
#    .select(avg(col("count"))).show()
print("Calculating rest avg")
df_rest = df_jaccard.where((col("jaccard") >= 0.3) & (col("jaccard") <= 0.7))
df_authors = df_rest.select(col("author1"))\
    .union(df_rest.select(col("author2").alias("author1"))).distinct()
df_joined = df.join(df_authors, col("author") == col("author1"))\
    .select(col("author"), col("title")).distinct()
df_joined.groupBy(col("author")).count()\
    .select(avg(col("count"))).show()
#df_jacc_authors = df_jaccard.select(col("author1").alias("author"))\
#    .union(df_jaccard.select(col("author2").alias("author")).distinct())
#df_all_authors = df.select(col("author")).distinct()
#df_rest = df_all_authors.subtract(df_jacc_authors)
#df_rest = df_rest.select(col("author").alias("author1"))
#df_joined = df.join(df_rest, col("author") == col("author1")).select("author", "title").distinct()
#df_joined.groupBy(col("author")).count().select(avg(col("count"))).show()
