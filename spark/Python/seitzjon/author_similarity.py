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

base_path = '/scratch/wikipedia-dump/wiki_small_'
logpath = '/home/ubuntu/BA_Project/log/jaccard_log.txt'
crosspath = '/scratch/wikipedia-dump/plots/jaccard/author_distance_cross/'
hashpath = '/scratch/wikipedia-dump/plots/jaccard/author_distance_hash/'

filenames = []
bot_names = ['Bot', 'Bots']
schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])
mode = 'dist'
jaccard_method = 'cross'
minval = 0.0
maxval = 1.0
activity = "all"

def draw_histogram(df, jaccard_method):
    fig, axes = plt.subplots()
    fig.set_size_inches(20, 20)
    hist(axes, [df], bins=20, color=['red'])
    if mode == 'sim':
    	axes.set_xlabel('Jaccard Ähnlichkeit')
    else:
        axes.set_xlabel('Jaccard Distanz')
    axes.set_ylabel('Anzahl der Autoren')
    if len(filenames) == 1:
        name_len = len(filenames[0])
        if name_len == len(base_path) + 6:
            file_no = filenames[0][len(base_path):len(base_path) + 1]
        elif name_len == len(base_path) + 7:
            file_no = filenames[0][len(base_path):len(base_path) + 2]
        else:
            print("Error while plotting: filenumber")
            return
        suffix = "author_" + mode + '_' + jaccard_method + '_f_' + file_no + '.png'
    else:
        suffix = "author_" + mode + '_' + jaccard_method + '_' + str(len(filenames)) + '_files.png'

    if jaccard_method == "cross":
        path = crosspath + suffix
    elif jaccard_method == "hash":
        path = hashpath + suffix

    plt.savefig(path)

def save_to_log(workers, files, duration, description):
    file = open(logpath, '+a')
    output = '{} {} {} {} {}\n'.format(workers, 16, files, duration, description)
    file.write(output)
    file.close()

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
parser.add_argument("--mode", help="sim for similarity, dist for distance")
parser.add_argument("--filenumber", help="filenumber of the file to load")
parser.add_argument("--jaccmethod", help="cross for simple crossjoin, hash for min hashing")
parser.add_argument("--minval", help="minimum value that will be plotted")
parser.add_argument("--maxval", help="maximum value that will be plotted")
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

#if args.activity:
#    activity = args.active

df = load_to_spark.main_init_df(filenames)

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_res)

#if activity == "active":
#    df_grouped = df_t_user.groupBy(col("author").alias("author1")).count()
#    df_grouped = df_grouped.where(col("count") > 10)
#    df_t_user = df_t_user.join(df_grouped, col("author") == col("author1")).select(col("author"), col("title"))
#if activity == "inactive":
#    df_grouped = df_t_user.groupBy(col("author").alias("author1")).count()
#    df_grouped = df_grouped.where(col("count") <= 10)
#    df_t_user = df_t_user.join(df_goruped, col("author") == col("author1")).select(col("author"), col("title"))

#select random authors
print("Selecting sample")
count = df_t_user.count() #count 1
#df_t_user = df_t_user.sample(False, fraction= 100000.0 / count, seed=int(round(time.time() * 1000)))
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
    df_hist = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "author", "title", mode=mode, minval=minval, maxval=maxval).select(col("jaccard"))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "cross")
elif jaccard_method == 'hash':
    print("Calculating jaccard using min hashing")
    #calculate with min-hashing
    df_hist = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "author", "title", mode=mode, minval=minval, maxval=maxval).select(col("jaccard"))
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "hash")
elif jaccard_method == 'both':
    print("Calculating jaccard using both methods")
    #calculate with crossjoin
    df_hist = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "author", "title", mode=mode, minval=minval, maxval=maxval).select(col("jaccard"))
    #draw histogram
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "cross")
    #calculate with min-hashing
    df_hist = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "author", "title", mode=mode, minval=minval, maxval=maxval).select(col("jaccard"))
    #draw histogram
    print("Hist count", df_hist.count())
    draw_histogram(df_hist, "hash")
else:
    print("Unrecognized jaccard method:", jaccard_method)

df_authors = df_t_user.select(col("author"))
all_count = df_authors.crossJoin(df_authors).count() - df_authors.count()

print("All values:", all_count)
print("Done")
