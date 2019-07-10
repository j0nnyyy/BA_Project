import matplotlib.pyplot as plt
import numpy as np
from pyspark_dist_explore import hist
from pyspark.sql import Window
from pyspark.sql.functions import col, window, row_number
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
import load_to_spark
import jaccard_similarity
import hotspot_detection
import argparse

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
filenames = []
to_compare = "title"

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
parser.add_argument("--filecount")
parser.add_argument("--compare")
args = parser.parse_args()

if args.filecount:
    filecount = int(args.filecount)
    if filecount > 1:
        for i in range(1, filecount + 1):
            filenames.append(base_path + str(i) + ".json")
    elif args.filenumber:
        filenames.append(base_path + args.filenumber + ".json")
    else:
        filenames.append(base_path + "1.json")
elif args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

if args.compare:
    to_compare = args.compare

def init_df():
    #df = load_to_spark.init_article_hotspot_df(filenames)
    df = load_to_spark.get_samples(filenames, 5000).select("title", "author", col("editTime").alias("timestamp"))
    df = df.where(col("author").isNotNull())
    df_bots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(df_bots)
    print(df.groupBy(col("title")).count().count())
    return df

def create_ids(df):
    df_titles = df.select("title").distinct()
    windowspec = Window.orderBy("title")
    df_ids = df_titles.withColumn("id", row_number().over(windowspec))
    df_ids = df_ids.withColumn("id", col("id") - 1)
    count = df_ids.count()
    return df_ids, count

def create_vectors(df, vector_length):
    df_vectors = df.rdd.map(lambda r: (r["window"], float(r['id']))).groupByKey()\
        .map(lambda r: sparse_vec(r, vector_length)).toDF()
    return df_vectors

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    ones = [1.0 for i in range(length)]
    return r[0], Vectors.sparse(count, list, ones)

def draw_histogram(list):
    bins = np.linspace(-1,1,20)
    plt.xlim([-1,1])
    fig = plt.figure()
    fig.set_size_inches(20, 20)
    plt.rcParams.update({'font.size': 28})
    plt.yscale("log")
    plt.xlabel("Pearson Korrelation")
    plt.ylabel("Anzahl der Titelpaare")
    plt.hist(list, bins, color="red")
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/real_" + to_compare +"_hotspot_correlation.png")

def jaccard(df):
    df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_hotspots, to_compare, "window")
    df_hist = df_jaccard.select("jaccard")
    draw_histogram(df_hist)

df = init_df()
df_hotspots = hotspot_detection.sliding_window_hotspots_by_time(df, type=to_compare)
#df_hotspots.cache()
df_hotspots.show()

df_ids, count = create_ids(df)
##id|window|<type>|rev_count
df_hotspots = df_hotspots.join(df_ids, "title")

df_vectors = create_vectors(df_hotspots, count)
df_vectors.show()
df_needed = df_vectors.select(col("_1").alias("window"), col("_2").alias("features")).select("features")
df_needed = df_needed.repartition(200)
df_needed.cache()
df_needed.show()

corr = Correlation.corr(df_needed, "features").head()
tmp = corr[0].toArray()

correlations = []
for i in range(len(tmp)):
    for j in range(i + 1, len(tmp[i])):
        if tmp[i][j] != 'nan':
            correlations.append(tmp[i][j])
            if tmp[i][j] < -0.5:
                print("Hello")

draw_histogram(correlations)
