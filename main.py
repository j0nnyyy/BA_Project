from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import from_unixtime, size, col, udf, explode
import datetime

filename = "short.json"

slen = udf(lambda s: len(s), IntegerType())

def createDataFrame(filename):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.load(filename, format="json")
    return df

# extract fields from array column
def init_df(df):
    columns_to_drop = ['redirect', 'ns', 'revision', 'date']
    df = df.withColumn("revision", explode("revision"))\
        .select("*",
        col("revision")["comment"].alias("comment"),
        col("revision")["contributor"]["username"].alias("author"),
        col("revision")["contributor"]["id"].alias("authorID"),
        col("revision")["timestamp"].alias("date"))\
        .withColumn('timestamp', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))

    #countdf = df.withColumn("revision", select('*', size('revision').alias('revision_length')))
    #countdf.show()

    df_res = df.drop(*columns_to_drop)
    return df_res

# total number of edits per author
def numbder_of_edits_per_author(df):
    print("Number of edits per author")
    authors = df.groupBy("author").count().orderBy("count", ascending=False)
    authors.show()

# number of all authors per each article
def number_of_authors_per_article(df):
    df.groupBy("title", "author").agg({"author": "count"}).show()

def convert_to_timestamp(date_text):
    return datetime.datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")

# number of authors per timestamp
def number_of_authors_per_timestamp(df, startDate, endDate):
    startDate = convert_to_timestamp(startDate)
    endDate = convert_to_timestamp(endDate)
    print(startDate)
    print(endDate)
    df.groupBy("author").agg({"author": "count"}).filter(col("timestamp").isin([startDate, endDate])).show()


df = createDataFrame(filename)

df_init = init_df(df)
df_init.show()
numbder_of_edits_per_author(df_init)

number_of_authors_per_article(df_init)

number_of_authors_per_timestamp(df_init, '2001-01-21 03:00:00', '2001-06-03 23:00:00')

#df2 = df_init.withColumn("revision_length", slen(df.revision))
#df2.show()

'''

# get length of edit per article
print("Size of edit per article")
edit_len = df.select(size(df.revision)).collect()
print(edit_len)

# number of unique authors
#distinctAuthors = df.groupBy("author").agg(countDistinct("author"))
#distinctAuthors.show()

'''