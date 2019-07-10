
from pyspark import SparkContext
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col, length, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import load_to_spark
import time
import argparse

logpath = '/home/ubuntu/BA_Project/log/perf_core_log.txt'
base_path = '/scratch/wikipedia-dump/wiki_small_'
f_big = '/scratch/wikipedia-dump/wikiJSON.json'

filenames = []

#for i in range(1, 6):
#    f_name = base_path + str(i) + '.json'
#    filenames.append(f_name)

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
parser.add_argument("--cores", help="the used core count")
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
    tmp = df.groupby("yearmonth", "title")
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
    print(tmp.count())
    end2 = time.time()
    dur1 = end1 - start
    dur2 = end2 - start
    return dur1, dur2

def save_to_log(file_count, worker_count, duration, description):
    global cores
    file = open(logpath, 'a+')
    output = '{} {} {} {} {}\n'.format(worker_count, cores, file_count, duration, description)
    file.write(output)

def test_select():
    dur1, dur2 = select(df)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'select')
    save_to_log(file_count, worker_count, dur2, 'select_count')
    print('select test complete')

def test_filter():
    dur1, dur2 = filter(df)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'filter')
    save_to_log(file_count, worker_count, dur2, 'filter_count')
    print('filter test complete')

def test_groupby(df):
    dur1, dur2 = filter(df)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'groupby')
    save_to_log(file_count, worker_count, dur2, 'groupby_count')
    print('groupby test complete')

def test_crossjoin(df1, df2):
    dur1, dur2 = crossjoin(df1, df2)
    file_count = len(filenames)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
#    save_to_log(file_count, worker_count, dur1, 'crossjoin')
    save_to_log(file_count, worker_count, dur2, 'crossjoin_count')
    print('crossjoin test complete')

abs_start_time = time.time()

df = load(filenames)
df = df.withColumn('revision', explode(df.revision))
test_select()
test_filter()

#prepare groupby data
df2 = load_to_spark.main_init_df(filenames)
df_monthly_ts = df2.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))

test_groupby(df_monthly_ts)

#prepare crossjoin data
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "128g") \
    .getOrCreate()
df_gn = load_to_spark.init(filenames)
df_monthly_ts = df_monthly_ts.groupBy("yearmonth", "title").count().orderBy(desc("count"))
df2 = df2.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))
df_monthly = df2.groupBy("yearmonth", "title").count().orderBy(desc("count"))
min_date, max_date = df_monthly_ts.select(min_("yearmonth").cast("long"), max_("yearmonth").cast("long")).first()
data = [(min_date, max_date)]
df_dates = spark.createDataFrame(data, ["minDate", "maxDate"])
df_min_max_date = df_dates.withColumn("minDate", col("minDate").cast("timestamp")).withColumn("maxDate", col("maxDate").cast("timestamp"))
df_groups = df_gn.select("title").distinct()
df_formatted_ts = df_min_max_date.withColumn("monthsDiff", f.months_between("maxDate", "minDate"))\
    .withColumn("repeat", f.expr("split(repeat(',', monthsDiff), ',')"))\
    .select("*", f.posexplode("repeat").alias("date", "val"))\
    .withColumn("date", f.expr("add_months(minDate, date)"))\
    .withColumn("yearmonth", f.concat(f.year("date"), f.lit('-'), format_string("%02d", f.month("date"))))\
    .select('yearmonth')

test_crossjoin(df_groups, df_formatted_ts)