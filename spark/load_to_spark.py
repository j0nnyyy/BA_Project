from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime, col, desc, explode

#filename = '/scratch/wikipedia-dump/wikiJSON.json'
#filename = '/scratch/wikipedia-dump/wiki_small_5.json'
filename = '/scratch/wikipedia-dump/XML_JSON.json'
#filename = '/scratch/wikipedia-dump/wiki_small_old.json'

sc = None

def create_dataframe(filename):
    global sc
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    sc = spark.sparkContext
    df = spark.read.load(filename, format="json")
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
    return create_dataframe(filename)


def main_init_df():
    df = create_dataframe(filename)
    return test(df)

def main_init_df_test():
    df = create_dataframe(filename)
    return test(df)
