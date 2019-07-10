from pyspark.sql.functions import col, avg, year, month, window, lit
import matplotlib.pyplot as plt
import jaccard_similarity
import load_to_spark
import argparse
import time

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
bots = ["Bot", "Bots"]

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
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

def init_df():
    df = load_to_spark.init_article_hotspot_df(filenames)
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df_authors = df.subtract(df_bots)
    return df_authors

def select_samples(df, sample_count):
    df_samples = df.sample(False, fraction=sample_count / df.count(), seed=int(round(time.time())))
    return df_samples

def draw_plot(x, y):
    plt.xlabel("Months since epoch")
    plt.ylabel("avg(jaccard_distance)")
    plt.plot(x, y)
    plt.savefig("/scratch/wikipedia-dump/plots/hotspots/article_jaccard_same_time.png")

df_authors = init_df()
#df_authors = select_samples(df_authors, 10.0)
df_windowed = df_authors.groupBy(window(col("timestamp"), "4 weeks")).count()
rows = df_windowed.collect()

df_all = None

for i in range(len(rows)):
    print(rows[i])
    df_filtered = df_authors.where(col("timestamp").between(rows[i]["window"]["start"], rows[i]["window"]["end"]))
    df_filtered.cache()
    df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_filtered, "title", "author")
    months_since_epoch = (rows[i]["window"]["start"].year * 12) + rows[i]["window"]["start"].month
    if df_jaccard != None:
        df_avg = df_jaccard.withColumn("months_since_epoch", lit(months_since_epoch)).groupBy(col("months_since_epoch")).avg("jaccard")
        df_avg = df_avg.select(col("months_since_epoch"), col("avg(jaccard)").alias("avg"))
        if df_all == None:
            df_all = df_avg
        else:
            df_all = df_all.union(df_avg)
    print("Done with", i, "out of", len(rows))

df_all = df_all.groupBy(col("months_since_epoch")).avg("avg").select("months_since_epoch", col("avg(avg)").alias("avg"))
df_all.show()
rows = df_all.collect()
x = []
y = []

for i in range(len(rows)):
    x.append(rows[i]["months_since_epoch"])
    y.append(rows[i]["avg"])

draw_plot(x, y)
