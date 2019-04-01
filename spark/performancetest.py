from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col
import time
import argparse

logpath = '/home/ubuntu/BA_Project/log.txt'
base_path = '/scratch/wikipedia-dump/wiki_small_'
f_big = '/scratch/wikipedia-dump/wikiJSON.json'

filenames = []

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
args = parser.parse_args()
if args.filecount:
    count = int(filecount)
    for i in xrange(1, count + 1):
        f_name = base_path + str(i) + '.json'
        filenames.append(f_name)
else:
    #load only one file to prevent errors
    f_name = base_path + '1.json'
    filenames.append(f_name)

sc = None
df = None

def load(filenames)
	global sc
    spark = SparkSession \
        .builder \
        .appName("Load") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.load(filenames, format="json")
    return df
	
def select(df):
	df.select("title")
	
def filter(df):
	df.filter(length(df.title) > 15)
	
def groupBy(df):
	df.groupby(
	
def crossjoin(df1, df2):
	df1.crossJoin(df2)
	
def save_to_log(file_count, worker_count, duration, description):
	file = open(logpath, 'a+')
	output = '{} {} {} {}\n'.format(worker_count, file_count, duration, description)
	file.write(output)

def test_load():
	global df
	start_time = time.time()
	df = load(filenames)
	end_time = time.time()
	file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	duration = end_time - start_time
	description = 'load'
	save_to_log(file_count, worker_count, duration, description)
	print('load test complete')

def test_select():
	start_time = time.time()
	select(df)
	end_time = time.time()
	file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	duration = end_time - start_time
	description = 'select'
	save_to_log(file_count, worker_count, duration, description)
	print('select test complete')
	
def test_filter():
	start_time = time.time()
	filter(df)
	end_time = time.time()
	file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	duration = end_time - start_time
	description = 'filter'
	save_to_log(file_count, worker_count, duration, description)
	print('filter test complete')

def test_groupby(df):
	start_time = time.time()
	filter(df)
	end_time = time.time()
	file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	duration = end_time - start_time
	description = 'groupby'
	save_to_log(file_count, worker_count, duration, description)
	print('groupby test complete')
	
def test_crossjoin(df1, df2):
	start_time = time.time()
	crossjoin(df1, df2)
	end_time = time.time()
	file_count = len(filenames)
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
df2 = load
df_monthly_ts = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))
	
test_groupby(df_monthly_ts)

#prepare crossjoin data
df_monthly_ts = df_monthly_ts.groupBy("yearmonth", "title").count().orderBy(desc("count"))
df = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))
df_monthly = df.groupBy("yearmonth", "title").count().orderBy(desc("count"))
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
file_count = len(filenames)
worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
description = 'perftest'
save_to_log(file_count, worker_count, abs_duration, description)
    