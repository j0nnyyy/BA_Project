from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime, col, explode

filename = '../wikiJSON.json'

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
        col("revision")["contributor"]["id"].alias("authorID"),
        col("revision")["timestamp"].alias("date"))\
        .withColumn('timestamp', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

    df_res = df.drop(*columns_to_drop)
    return df_res


def main_init_df():
    df = createDataFrame(filename)
    return extract_df_from_revisions(df)
