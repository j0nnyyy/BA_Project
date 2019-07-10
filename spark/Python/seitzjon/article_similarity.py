from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, explode
import jaccard_similarity
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
crosspath = '/scratch/wikipedia-dump/plots/jaccard/article_distance_cross/'
hashpath = '/scratch/wikipedia-dump/plots/jaccard/article_distance_hash/'
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

def draw_histogram(df, jaccard_method):
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
        suffix = "article_" + mode + '_' + jaccard_method + '_f_' + file_no + '.png'
    else:
        suffix = "article_" + mode + '_' + jaccard_method + '_' + str(len(filenames)) + '_files.png'
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
#df_t_user = df_t_user.sample(False, fraction=100000.0 / count, seed=int(round(time.time() * 1000)))
df_t_user.cache()
print(df_t_user.count()) #count 2

if args.jaccmethod:
    jaccard_method = args.jaccmethod

if jaccard_method == 'cross':
    print("Calculating jaccard using crossjoin")
    df_res = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "title", "author", minval=minval, maxval=maxval).select(col("jaccard"))
elif jaccard_method == 'hash':
    print("Calculating jaccard using min hashing")
    df_res = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "title", "author", minval=minval, maxval=maxval).select(col("jaccard"))
elif jaccard_method == 'both':
    print("Calculating jaccard using both methods")
    df_res_cross = jaccard_similarity.jaccard_with_crossjoin(df_t_user, "title", "author", minval=minval, maxval=maxval).select(col("jaccard"))
    df_res_hash = jaccard_similarity.jaccard_with_min_hashing(df_t_user, "title", "author", minval=minval, maxval=maxval).select(col("jaccard"))
else:
    print("Unrecognized jaccard method:", jaccard_method)

if jaccard_method == "both":
    draw_histogram(df_res_cross, "cross")
    draw_histogram(df_res_hash, "hash")
else:
    draw_histogram(df_res, jaccard_method)

df_titles = df.select(col("title")).distinct()
count = df_titles.count()
print("All title pairs", ((count * count) - count) / 2)

print("Done")
