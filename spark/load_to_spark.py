from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime, col, desc, explode

filename = '/home/ubuntu/BA_Project/XML_JSON.json'
#filename = '/scratch/wikipedia-dump/wikiJSON.json'

def create_dataframe(filename):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
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


def init():
    return create_dataframe(filename)


def main_init_df():
    df = create_dataframe(filename)
    return extract_df_from_revisions(df)



