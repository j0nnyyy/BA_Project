from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, explode
import load_to_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import argparse
import time

base_path = '/scratch/wikipedia-dump/wiki_small_'
logpath = '/home/ubuntu/BA_Project/log/jaccard_log.txt'
plotpath = '/home/ubuntu/article_'
filenames = []
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

def jaccard_with_crossjoin(df_t_user):
    global minval
    global maxval

    #crossjoin to get all possible pairs
    print("Self joining...")
    df1 = df_t_user.select(col("title").alias("title1"), col("author").alias("author1"))
    df2 = df_t_user.select(col("title").alias("title2"), col("author").alias("author2"))
    df_joined = df1.crossJoin(df2)
    df_joined.show()
    df_joined = df_joined.where(col("title1") < col("title2"))
    df_joined.show() #show 2
    print("Join complete")

    print("Calculating all authors per article pair")
    df_all = df_joined.groupBy(col("title1"), col("title2")).count()\
        .select(col("title1").alias("td1"), col("title2").alias("td2"), col("count").alias("dis")).distinct()
    df_all.show()
    print("df_all", df_all.count())
    print("Calculating common authors")
    df_common = df_joined.where(col("author1") == col("author2")).groupBy(col("title1"), col("title2")).count()\
        .select(col("title1").alias("tc1"), col("title2").alias("tc2"), col("count").alias("con")).distinct()
    df_common.show()
    print("df_common", df_common.count())
    print("Calculating rest")
    df_rest = df_joined.where(col("author1") != col("author2")).groupBy(col("title1"), col("title2")).count()\
        .select(col("title1").alias("tc1"), col("title2").alias("tc2"))\
        .withColumn("con", f.lit(0))
    df_rest.show()
    print("df_rest", df_rest.count())
    print("Union common and rest")
    df_common = df_common.union(df_rest).groupBy(col("tc1"), col("tc2")).sum()
    df_common = df_common.select(col("tc1"), col("tc2"), col("sum(con)").alias("con"))
    df_common.show()
    print("df_common", df_common.count())
    print("Joining over both articles")
    df_all = df_all.join(df_common, (col("td1") == col("tc1")) & (col("td2") == col("tc2")))\
        .select(col("td1"), col("td2"), col("dis"), col("con"))
    df_all.show()
    print("df_all", df_all.count())
    print("Subtracting duplicates")
    df_all = df_all.withColumn("dis", col("dis") - col("con"))
    df_all.show()
    print("df_all - no duplicates", df_all.count())

    #calculate jaccard
    print("Calculating jaccard")
    if mode == 'sim':
        df_jaccard = df_all.withColumn("jaccard", col("con") / col("dis"))
    elif mode == 'dist':
        df_jaccard = df_all.withColumn("jaccard", 1.0 - (col("con") / col("dis")))
    print("df_jaccard", df_jaccard.count())

    print("Drawing Jaccard")
    df_hist = df_jaccard.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print("Hist count", df_hist.count())

    draw_histogram(df_hist, "cross")

def jaccard_with_min_hashing(df_t_user):
    global minval
    global maxval

    #get titles
    df_authors = df_t_user.select(df_t_user.author).distinct()
    
    #create ids for each title
    print("Creating ids")
    windowSpec = W.orderBy("author")
    df_authors = df_authors.withColumn("id", f.row_number().over(windowSpec))
    
    #window function moved df_titles to single partition --> repartition
    df_authors.repartition(200)
    df_authors.count()
    
    #join dataframes to get author/id pairs
    print("Joining...")
    df1 = df_t_user.alias("df1")
    df2 = df_authors.alias("df2")
    df_joined = df1.join(df2, col('df1.author') == col('df2.author')).select(col('df1.title'), col('df2.id'))
    df_joined.show()
    print("Join Complete")
    
    #create binary vectors
    print("Creating vectors")
    count = df_authors.count() + 10
    max_index = int(df_authors.select(col("id")).orderBy(desc("id")).first()["id"])
    size = max(count, max_index)
    df_joined = df_joined.rdd.map(lambda r: (r['title'], float(r['id']))).groupByKey().map(lambda r: sparse_vec(r, size)).toDF()
    
    df_res = df_joined.select(col('_1').alias('title'), col('_2').alias('features'))
    df_res.show()
    df_res = df_res.repartition(2000)
    
    print("Creating model")
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
    model = mh.fit(df_res)
    model.transform(df_res).show()
    
    print("Calculating Jaccard")
    df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 1.0, distCol="jaccard")
    
    print("Selecting needed columns")
    df_jacc_dist = df_jacc_dist.select(col("datasetA.title").alias("title1"),
                col("datasetB.title").alias("title2"),
                col("jaccard")).where(col("title1") < col("title2"))
    print(df_jacc_dist.count())
    
    print("Drawing Jaccard")
    df_hist = df_jacc_dist.select("jaccard").where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    
    if mode == "sim":
        df_hist = df_hist.withColumn("jaccard", 1.0 - col("jaccard"))

    draw_histogram(df_hist, "hash")

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
    hist(axes, [df], bins=20, color=['red'])
    if mode == 'sim':
    	axes.set_xlabel('Jaccard Ähnlichkeit')
    else:
        axes.set_xlabel('Jaccard Distanz')
    axes.set_ylabel('Anzahl der Artikel')
    if len(filenames) == 1:
        name_len = len(filenames[0])
        if name_len == len(base_path) + 6:
            file_no = filenames[0][len(base_path):len(base_path) + 1]
        elif name_len == len(base_path) + 7:
            file_no = filenames[0][len(base_path):len(base_path) + 2]
        else:
            print("Error while plotting: filenumber")
            return
        path = plotpath + mode + '_' + jaccard_method + '_f_' + file_no + '.png'
    else:
        path = plotpath + mode + '_' + jaccard_method + '_' + str(len(filenames)) + '_files.png'
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

if args.filecount:
    #multiple files have to be loaded
    count = int(args.filecount)
    print("Loading ", count, " files")
    for i in range(1, count + 1):
        f_name = base_path + str(i) + '.json'
        filenames.append(f_name)
else:
    #only one file has to be loaded
    if args.filenumber:
        #file to load was specified
        print("Loading file ", args.filenumber)
        f_name = base_path + args.filenumber + '.json'
    else:
        #load first file to prevent errors
        print("No file specified. Loading file 1")
        f_name = base_path + '1.json'
    filenames.append(f_name)

if args.mode:
    if args.mode == 'sim' or args.mode == 'dist':
        mode = args.mode
    else:
        print("Invalid mode: ", args.mode)
        print("Calculating similarity instead")

if args.minval:
    minval = float(args.minval)

if args.maxval:
    maxval = float(args.maxval)

df = load_to_spark.main_init_df(filenames)

#get all title/author pairs
print("Selecting real users")
df_t_a = df.select(df.title, df.author).where(col('author').isNotNull())
df_t_bot = df_t_a.where(df_t_a.author.rlike('|'.join(bot_names)))
df_t_user = df_t_a.subtract(df_t_bot)
df_t_user = df_t_user.distinct()
df_t_user.show() #show 1
print("Real Users Selected")

#select random authors
count = df_t_user.count() #count 1
df_t_user = df_t_user.sample(False, fraction= 10000.0 / count, seed=int(round(time.time() * 1000)))
df_t_user.cache()
print(df_t_user.count()) #count 2

if args.jaccmethod:
    jaccard_method = args.jaccmethod

if jaccard_method == 'cross':
    print("Calculating jaccard using crossjoin")
    jaccard_with_crossjoin(df_t_user)
elif jaccard_method == 'hash':
    print("Calculating jaccard using min hashing")
    jaccard_with_min_hashing(df_t_user)
elif jaccard_method == 'both':
    print("Calculating jaccard using both methods")
    jaccard_with_crossjoin(df_t_user)
    jaccard_with_min_hashing(df_t_user)
else:
    print("Unrecognized jaccard method:", jaccard_method)

print("Done")
