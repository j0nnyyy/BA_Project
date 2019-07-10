from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, monotonically_increasing_id, explode
import load_to_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import jaccard_similarity
import argparse
import time

filenames = ['/scratch/wikipedia-dump/wiki_small_1.json']
bot_names = ['Bot', 'Bots']
schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])
mode = 'sim'
jaccard_method = 'cross'
minval = 0.0
maxval = 1.0
activity = "all"

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    ones = [1.0 for i in range(length)]
    return r[0], Vectors.sparse(count, list, ones)

def draw_histogram(df, jaccard_method):
    global plotpath
    global mode
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df], bins=20, color=['red'])
    axes[0, 0].set_xlabel('Jaccard Koeffizient')
    axes[0, 0].set_ylabel('Anzahl der Autoren')
    plt.savefig('Jaccard_Similarity')

    
df = load_to_spark.main_init_df()

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
parser.add_argument("--mode", help="sim for similarity, dist for distance")
parser.add_argument("--filenumber", help="filenumber of the file to load")
parser.add_argument("--jaccmethod", help="cross for simple crossjoin, hash for min hashing")
parser.add_argument("--minval", help="minimum value that will be plotted")
parser.add_argument("--maxval", help="maximum value that will be plotted")
parser.add_argument("--activity", help="active for active authors, inactive for inactive authors, all for all authors")
args = parser.parse_args()

#get titles
df_titles = df.select(df.title).distinct()

#create ids for each title
windowSpec = W.orderBy("title")
df_titles = df_titles.withColumn("id", f.row_number().over(windowSpec))

#join dataframes to get author/id pairs
df1 = df_t_user.alias("df1")
df2 = df_titles.alias("df2")
df_joined = df1.join(df2, col('df1.title') == col('df2.title')).select(col('df1.author'), col('df2.id'))

#create binary vectors
count = df_joined.count()
df_joined = df_joined.rdd.map(lambda r: (r['author'], float(r['id']))).groupByKey().map(lambda r: sparse_vec(r, count)).toDF()

if args.activity:
    activity = args.active

df = load_to_spark.main_init_df(filenames)

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_res)

if activity == "active":
    df_grouped = df_t_user.groupBy(col("author").alias("author1")).count()
    df_grouped = df_grouped.where(col("count") > 10)
    df_t_user = df_t_user.join(df_grouped, col("author") == col("author1")).select(col("author"), col("title"))
if activity == "inactive":
    df_grouped = df_t_user.groupBy(col("author").alias("author1")).count()
    df_grouped = df_grouped.where(col("count") <= 10)
    df_t_user = df_t_user.join(df_goruped, col("author") == col("author1")).select(col("author"), col("title"))

#select random authors
print("Selecting sample")
count = df_t_user.count() #count 1
df_t_user = df_t_user.sample(False, fraction= 10000.0 / count, seed=int(round(time.time() * 1000)))
df_t_user.cache()
print(df_t_user.count()) #count 2

df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 0.6, distCol="JaccardDistance")\
    .select(col("datasetA.author").alias("author1"),
            col("datasetB.author").alias("author2"),
            col("JaccardDistance")).filter("JaccardDistance != 0").orderBy(desc("JaccardDistance"))
df_jacc_dist.show()

if jaccard_method == 'cross':
    print("Calculating jaccard using crossjoin")
    #calculate with crossjoin
    df_jaccard = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "author", "title", mode=mode, jaccard_method="cross", maxval=0.9)
    print("Drawing Jaccard")
    df_hist = df_jaccard.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "cross")
elif jaccard_method == 'hash':
    print("Calculating jaccard using min hashing")
    #calculate with min-hashing
    df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "author", "title", mode=mode, jaccard_method="hash", maxval=0.9)
    print("Drawing Jaccard")
    df_hist = df_jaccard.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "hash")
elif jaccard_method == 'both':
    print("Calculating jaccard using both methods")
    #calculate with crossjoin
    df_jaccard = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "author", "title", mode=mode, jaccard_method="cross", maxval=0.9)
    #draw histogram
    print("Drawing Jaccard")
    df_hist = df_jaccard.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "cross")
    #calculate with min-hashing
    df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "author", "title", mode=mode, jaccard_method="cross", maxval=0.9)
    #draw histogram
    print("Drawing Jaccard")
    df_hist = df_jaccard.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "hash")
else:
    print("Unrecognized jaccard method:", jaccard_method)

draw_histogram(df_hist)