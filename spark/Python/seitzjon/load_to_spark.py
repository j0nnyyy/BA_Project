import codecs
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql.functions import from_unixtime, col, desc, explode

import argparse
import time

base_path = '/scratch/wikipedia-dump/wiki_small_'

f_big = '/scratch/wikipedia-dump/wikiJSON.json'

sc = None
spark = None
filenames = []

dump_schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",TimestampType(),True)]),True), \
    True),StructField("title",StringType(),True)])

category_schema = StructType([StructField("category",
    ArrayType(StringType(),True),True),StructField("id", StringType(),True)])

def create_session():
    global sc
    global spark
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.executor.memory", "128g") \
        .config("spark.speculation", "true") \
        .config("spark.sql.shuffle.partitions", "100") \
        .getOrCreate()
    sc = spark.sparkContext

def create_dataframe(filenames):
    create_session()
    df = spark.read.format("json").load(filenames, schema=dump_schema)
    return df

def create_category_df():
    if spark == None:
        create_session()

    df = spark.read.json("/scratch/wikipedia-dump/categorylinks.json", schema=category_schema)
    df = df.withColumn("category", explode("category"))
    return df

def save_df(df, path):
    df.write.mode("append").json(path)

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

def get_samples(filenames, samples=20000):
    df = create_dataframe(filenames)
    print(df.count())
    df_samples = df.sample(False, fraction = samples / df.count(), seed=int(round(time.time()))).limit(samples)
    df_samples.cache()
    return extract_df_from_revisions(df_samples)

def get_author_samples(filenames, samples=20000):
    df = create_dataframe(filenames)
    df = extract_df_from_revisions(df)
    df_authors = df.select(col("author")).distinct()
    df_authors = df_authors.sample(False, fraction = samples / df_authors.count(), seed=int(round(time.time()))).limit(samples)
    df_joined = df_authors.join(df, "author")
    df_joined.cache()
    return df_joined

def init(filenames):
    return create_dataframe(filenames)

def main_init_df(filenames):
    df = create_dataframe(filenames)
    return extract_df_from_revisions(df)

def init_article_hotspot_df(filenames):
    df = create_dataframe(filenames)
    df = df.withColumn("revision", explode("revision"))\
        .select(col("revision")["id"].alias("revID"),
                col("revision")["contributor"]["username"].alias("author"),
                col("revision")["timestamp"].alias("timestamp"),
                col("title"),
                col("id"))
    return df
