from pyspark import StorageLevel
from pyspark import SparkContext
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col, length, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import load_to_spark
import time
import argparse

logpath = '/home/ubuntu/BA_Project/log/same_diff_log.txt'
base_path = '/scratch/wikipedia-dump/wiki_small_'
f_big = '/scratch/wikipedia-dump/wikiJSON.json'

filenames = []

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
parser.add_argument("--cores", help="the used core count")
parser.add_argument("--same", help="master and slave on same node")
args = parser.parse_args()
if args.filecount:
    count = int(args.filecount)
    for i in range(1, count + 1):
        f_name = base_path + str(i) + '.json'
        filenames.append(f_name)
else:
    #load only one file to prevent errors
    f_name = base_path + '1.json'
    filenames.append(f_name)

cores = 16
if args.cores:
    cores = int(args.cores)

schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])
    
sc = None
df = None

def load(filenames):
    global sc
    spark = SparkSession \
        .builder \
        .appName("Load") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.load(filenames, format="json", schema=schema)
    return df

def select(df):
    start = time.time()
    tmp = df.select(df.title, df.revision.contributor.username, df.revision.timestamp)
    end1 = time.time()
    tmp.count()
    end2 = time.time()
    dur1 = end1 - start
    dur2 = end2 - start
    return dur1, dur2
    
def filter(df):
    start = time.time()
    tmp = df.filter(length(df.title) > 15)
    end1 = time.time()
    tmp.count()
    end2 = time.time()
    dur1 = end1 - start
    dur2 = end2 - start
    return dur1, dur2

def groupBy(df):
    start = time.time()
    tmp = df.groupby("title").count()
    end1 = time.time()
    tmp.count()
    end2 = time.time()
    dur1 = end1 - start
    dur2 = end2 - start
    return dur1, dur2
    
def crossjoin(df1, df2):
    start = time.time()
    tmp = df1.crossJoin(df2)
    end1 = time.time()
    tmp.count()
    end2 = time.time()
    dur1 = end1 - start
    dur2 = end2 - start
    return dur1, dur2

def save_to_log(file_count, worker_count, duration, description):
    global cores
    file = open(logpath, 'a+')
    if args.same:
        if args.same == "True":
            description = description + "_same"
        elif args.same == "False":
            description = description + "_diff"
    output = '{} {} {} {} {}\n'.format(worker_count, cores, file_count, duration, description)
    file.write(output)

def test_select():
    dur1, dur2 = select(df)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'select')
#    save_to_log(file_count, worker_count, dur2, 'select_count')
    print('select test complete')

def test_filter():
    dur1, dur2 = filter(df)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'filter')
#    save_to_log(file_count, worker_count, dur2, 'filter_count')
    print('filter test complete')

def test_groupby(df):
    dur1, dur2 = filter(df)
    file_count = len(filenames)
    worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'groupby')
    save_to_log(file_count, worker_count, dur2, 'groupby_count')
    print('groupby test complete')

def test_crossjoin(df1, df2):
    dur1, dur2 = crossjoin(df1, df2)
    file_count = len(filenames)
    worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'crossjoin')
    save_to_log(file_count, worker_count, dur2, 'crossjoin_count')
    print('crossjoin test complete')

abs_start_time = time.time()

first_file_titles = 22288
df = load(filenames)
df_titles = df.select("title").limit(len(filenames * first_file_titles))
df_titles.persist(StorageLevel.DISK_ONLY)
df_titles.show()
df = df.withColumn('revision', explode(df.revision))
#test_select()
#test_filter()

df2 = load_to_spark.main_init_df(filenames)

load_to_spark.create_session()
first_file_titles = 22288
test_groupby(df2)
test_crossjoin(df_titles, df_titles)
df_titles.unpersist()
