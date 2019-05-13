from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
import argparse

logpath = '/home/ubuntu/BA_Project/log.txt'
base_path = '/scratch/wikipedia-dump/wiki_small_'
f_big = '/scratch/wikipedia-dump/wikiJSON.json'

filenames = []

parser = argparse.ArgumentParser()
parser.add_argument("--filecount", help="sets the number of files that will be loaded")
parser.add_argument("--schema", help="sets schema usage")
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

spark = SparkSession \
    .builder \
    .appName("performancetests") \
    .config("spark.executor.memory", "128g") \
    .getOrCreate()
sc = spark.sparkContext

schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])
    
def save_to_log(file_count, worker_count, duration, description):
    file = open(logpath, 'a+')
    output = '{} {} {} {} {}\n'.format(worker_count, 16, file_count, duration, description)
    file.write(output)
    
def load_no_schema(filenames)
    df = spark.read.load(filenames, format="json")

def load_schema(filenames)
    df = spark.read.load(filenames, format="json", schema=schema)
    
def test_load_no_schema():
    start = time.time()
    load_no_schema(filenames)
    end = time.time()
    duration = end - start
    description = "load_no_schema"
    file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	save_to_log(file_count, worker_count, duration, description)

def test_load_schema():
    start = time.time()
    load_schema(filenames)
    end = time.time()
    duration = end - start
    description = "load_no_schema"
    file_count = len(filenames)
	worker_count = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
	save_to_log(file_count, worker_count, duration, description)
    
if args.schema:
    test_load_schema()
else:
    test_load_no_schema()
    
file.close()
