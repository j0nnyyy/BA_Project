from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, explode, desc
import pyspark.sql.functions as f

filename = '../XML_JSON.json'
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def createDataFrame(filename):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.load(filename, format="json")
    return df


# extract fields from array column
def extract_df_from_revisions(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("revision", explode("revision"))\
        .select("*",
        #col("revision")["comment"].alias("comment"),
        col("revision")["contributor"]["username"].alias("author"),
        #col("revision")["contributor"]["id"].alias("authorID"),
        col("revision")["timestamp"].alias("date"))\
        .withColumn('editTime', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

    df_res = df.drop(*columns_to_drop)
    return df_res


def main_init_df():
    df = createDataFrame(filename)
    return extract_df_from_revisions(df)


df = main_init_df()
df.cache()
df.persist

NUM_DAYS = 3

windowSpec = Window.partitionBy("title").orderBy("editTime").rangeBetween(-NUM_DAYS, NUM_DAYS)
df.withColumn("occurrences", f.count("author").over(windowSpec)).show()
#df.show(df.count(), False)
