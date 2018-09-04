from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime, col, desc, explode
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

slen = udf(lambda s: len(s), IntegerType())

filename = '../myXML.json'


def create_dataframe(filename):
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
        .withColumn('editTime', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

    df_res = df.drop(*columns_to_drop)
    return df_res


def init():
    return create_dataframe(filename)


def main_init_df():
    df = create_dataframe(filename)
    return extract_df_from_revisions(df)


df = init()
print("Total revisions:")
df_totalEdits = df.withColumn("total revisions", slen(df.revision)).orderBy(desc("total revisions"))
df_totalEdits.cache()
df_totalEdits.persist
#df_totalEdits.show()
#df.show(df.count(), False)

print("Dataframe:")
df_extracted = main_init_df()
df_extracted.cache()
df_extracted.persist()
#df_extracted.show()


