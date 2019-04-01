from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime, col, desc, explode

import argparse

base_path = '/scratch/wikipedia-dump/wiki_small_'

f_big = '/scratch/wikipedia-dump/wikiJSON.json'

sc = None
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

def create_dataframe(filenames):
    global sc
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.format("json").load(filenames)
    return df


# extract fields from array column
def extract_df_from_revisions(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("revision", explode("revision"))\
        .select("*",
                col("revision")["contributor"]["username"].alias("author"),
                col("revision")["contributor"]["id"].alias("authorID"),
                col("revision")["timestamp"].alias("date"))\
        .withColumn('editTime', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

    df_res = df.drop(*columns_to_drop)
    return df_res

def test(df):
    columns_to_drop = ['redirect', 'ns', 'revisions']
    df.select(col("revision")["timestamp"].alias("date")).show()
    df = df.withColumn("revision", explode("revision"))\
	.select("*",
		col("revision")["contributor"]["username"].alias("author"),
		col("revision")["contributor"]["id"].alias("authorID"),
		col("revision")["timestamp"].alias("editTime"))

    df_res = df.drop(*columns_to_drop)
    return df_res

def init():
    return create_dataframe(filenames)


def main_init_df():
    df = create_dataframe(filenames)
    return test(df)

def main_init_df_test():
    df = create_dataframe(filenames)
    return test(df)
