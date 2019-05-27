from pyspark.sql.functions import col, datediff
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
import jaccard_similarity
import load_to_spark
import argparse
import time

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = []
bots = ["Bot", "Bots"]
mode = "dist"
jaccmethod = "cross"

def draw_histogram(df):
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes, [df], bins=50, color=['red'])
    axes.set_xlabel("Unterschied der Bearbeitungsdaten in Tagen")
    axes.set_ylabel('Anzahl der Autoren')
    plotpath = "/scratch/wikipedia-dump/plots/jaccard/date_diff_similar_authors.png"
    plt.savefig(plotpath)

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="number of files")
parser.add_argument("--filenumber", help="number of file to load")
parser.add_argument("--mode", help="sim or dist")
parser.add_argument("--jaccmethod", help="cross or hash")
args = parser.parse_args()

if args.filecount:
    count = int(filecount)
    for i in range(1, count + 1):
        path = base_path + str(i) + ".json"
        filenames.append(path)
elif args.filenumber:
    path = base_path + args.filenumber + ".json"
    filenames.append(path)
else:
    path = base_path + "1.json"
    filenames.append(path)

if args.mode:
    if mode == "dist" or mode == "sim":
        mode = args.mode
    else:
        print("Unrecognized mode:", args.mode, "Calculating distance instead")

df = load_to_spark.main_init_df(filenames)
df_t_user = df.select(col("author"), col("title"), col("editTime"))
df_t_user = df_t_user.where(col("author").isNotNull()).distinct()
print("Selecting bots")
df_bots = df_t_user.where(col("author").rlike("|".join(bots)))
print("Subtracting bots")
df_t_user = df_t_user.subtract(df_bots)
count = df_t_user.count()
print("Take sample data")
df_sample = df_t_user.sample(False, fraction=10000.0 / count, seed=int(round(time.time())))
df_sample.cache()

if args.jaccmethod:
    jaccmethod = args.jaccmethod
    if args.jaccmethod == "cross":
        df_res = jaccard_similarity.jaccard_with_crossjoin(df_sample, "author", "title", "dist", maxval=0.3)
    elif args.jaccmethod == "hash":
        df_res = jaccard_similarity.jaccard_with_min_hashing(df_sample, "author", "title", "dist", maxval=0.3)
    elif args.jaccmethod == "both":
        df_res_1 = jaccard_similarity.jaccard_with_crossjoin(df_sample, "author", "title", "dist", maxval=0.3)
        df_res_2 = jaccard_similarity.jaccard_with_min_hashing(df_sample, "author", "title", "dist", maxval=0.3)
    else:
        print("Unrecognized jaccard method:", args.jaccmethod, "Calculating with crossjoin instead")
        df_res = jaccard_similarity.jaccard_with_crossjoin(df_sample, "author", "title", "dist", maxval=0.3)
else:
    print("No jaccard method selected. Calculating using crossjoin")
    df_res = jaccard_similarity.jaccard_with_crossjoin(df_sample, "author", "title", "dist", maxval=0.3)

if jaccmethod == "both":
    print("not implemented yet")
else:
    df_res = df_res.select(col("author1").alias("a1"), col("author2").alias("a2"))
    df1 = df_sample.select(col("author").alias("author1"), col("title").alias("title1"), col("editTime").alias("editTime1"))
    df2 = df_sample.select(col("author").alias("author2"), col("title").alias("title2"), col("editTime").alias("editTime2"))
    df_joined = df1.join(df2, col("title1") == col("title2"))
    df_joined.cache()
    df_joined = df_joined.select(col("author1"), col("author2"), col("title1").alias("title"), col("editTime1"), col("editTime2"))
    df_joined = df_joined.withColumn("datediff", datediff(col("editTime1"), col("editTime2")))\
        .select(col("author1"), col("author2"), col("datediff"))
    df_joined = df_joined.join(df_res, (col("author1") == col("a1")) & (col("author2") == col("a2"))).select(col("author1"), col("author2"), col("datediff"))
    df_grouped = df_joined.groupBy(col("author1"), col("author2")).avg("datediff")
    df_diff = df_grouped.select(col("avg(datediff)"))
    draw_histogram(df_diff)
