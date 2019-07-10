from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import from_unixtime, col, desc, explode

import argparse

base_path = '/scratch/wikipedia-dump/wiki_small_'

f_big = '/scratch/wikipedia-dump/wikiJSON.json'

sc = None
filenames = []

schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])

def create_dataframe(filenames):
    global sc
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.executor.memory", "50g") \
        .config("spark.speculation", "true") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.format("json").load(filenames, schema=schema)
    return df

# extract fields from array column
def extract_df_from_revisions(df):
    columns_to_drop = ['redirect', 'ns', 'revision']
    df = df.withColumn("revision", explode("revision"))\
	.select("*",
		col("revision")["contributor"]["username"].alias("author"),
		col("revision")["contributor"]["id"].alias("authorID"),
		col("revision")["timestamp"].alias("editTime"))

    df_res = df.drop(*columns_to_drop)
    return df_res

def init(filenames):
    return create_dataframe(filenames)

def main_init_df(filenames):
    df = create_dataframe(filenames)
    return extract_df_from_revisions(df)
