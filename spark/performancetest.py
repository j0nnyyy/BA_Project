
from pyspark import SparkContext
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col, length
import load_to_spark
import time

filename = '/scratch/wikipedia-dump/wikiJSON.json'
#filename = ['/scratch/wikipedia-dump/wiki_small_1.json']
#filename = ['/scratch/wikipedia-dump/wiki_small_1.json', '/scratch/wikipedia-dump/wiki_small_2.json']
#filename = ['/scratch/wikipedia-dump/wiki_small_1.json', '/scratch/wikipedia-dump/wiki_small_2.json', '/scratch/wikipedia-dump/wiki_small_3.json']
#filename = ['/scratch/wikipedia-dump/wiki_small_1.json', '/scratch/wikipedia-dump/wiki_small_2.json', '/scratch/wikipedia-dump/wiki_small_3.json', '/scratch/wikipedia-dump/wiki_small_4.json']
#filename = ['/scratch/wikipedia-dump/wiki_small_1.json', '/scratch/wikipedia-dump/wiki_small_2.json', '/scratch/wikipedia-dump/wiki_small_3.json', '/scratch/wikipedia-dump/wiki_small_4.json', '/scratch/wikipedia-dump/wiki_small_5.json']

logpath = '/home/ubuntu/BA_Project/log.txt'

sc = None
df = None

def load(filename):
    global sc
    spark = SparkSession \
        .builder \
        .appName("Load") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.load(filename, format="json")
    return df

def select(df):
    df.select("title")

def filter(df):
    df.filter(length(df.title) > 15)

def groupBy(df):
    df.groupby("yearmonth", "title")

def crossjoin(df1, df2):
    df1.crossJoin(df2)

def save_to_log(file_count, worker_count, duration, description):
    file = open(logpath, 'a+')
    output = '{} {} {} {}\n'.format(worker_count, file_count, duration, description)
    file.write(output)

def test_load():
    global df
    start_time = time.time()
    df = load(filename)
    end_time = time.time()
    file_count = len(filename)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    duration = end_time - start_time
    description = 'load'
    save_to_log(file_count, worker_count, duration, description)
    print('load test complete')

def test_select():
    start_time = time.time()
    select(df)
    end_time = time.time()
    file_count = len(filename)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    duration = end_time - start_time
    description = 'select'
    save_to_log(file_count, worker_count, duration, description)
    print('select test complete')

def test_filter():
    start_time = time.time()
    filter(df)
    end_time = time.time()
    file_count = len(filename)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    duration = end_time - start_time
    description = 'filter'
    save_to_log(file_count, worker_count, duration, description)
    print('filter test complete')

def test_groupby(df):
    start_time = time.time()
    filter(df)
    end_time = time.time()
    file_count = len(filename)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    duration = end_time - start_time
    description = 'groupby'
    save_to_log(file_count, worker_count, duration, description)
    print('groupby test complete')

def test_crossjoin(df1, df2):
    start_time = time.time()
    crossjoin(df1, df2)
    end_time = time.time()
    file_count = len(filename)
    worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
    duration = end_time - start_time
    description = 'crossjoin'
    save_to_log(file_count, worker_count, duration, description)
    print('groupby test complete')

abs_start_time = time.time()

test_load()
test_select()
test_filter()

#prepare groupby data
df2 = load_to_spark.main_init_df()
df_monthly_ts = df2.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))

test_groupby(df_monthly_ts)

#prepare crossjoin data
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "128g") \
    .getOrCreate()
df_gn = load_to_spark.init()
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

abs_end_time = time.time()
abs_duration = abs_end_time - abs_start_time
file_count = len(filename)
worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
description = 'perftest'
save_to_log(file_count, worker_count, abs_duration, description)
