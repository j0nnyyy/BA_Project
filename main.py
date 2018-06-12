from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_unixtime

filename = "output.json"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.load(filename, format="json")

# extract some fields from revisions array
df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
    .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
    .withColumn("date", df["revision"].getItem(0).timestamp)

df.createOrReplaceTempView("wiki")
sqlDF = spark.sql("SELECT * FROM wiki")
#sqlDF.select("id", "title", "revision", "author", "authorID", "date").show()

# convert date to timestamp format
df_date = sqlDF.withColumn('date_format', from_unixtime('date', 'yyyy-MM-dd HH:mm:ss').cast(TimestampType()))
df_date.createOrReplaceTempView("date")
sqlDF_date = spark.sql("SELECT * FROM date")
sqlDF_date.select("id", "title", "revision", "author", "authorID", "date_format").show()
#df_date.printSchema()


# number of edits per author
print("Number of edits per author")
authors = df.groupBy("author").count().orderBy("count", ascending=False)
authors.show()

# get length of edit per article
#print("Size of edit per article")
#edit_len = df.select(size(df.revision)).collect()
#print(edit_len)

# number of unique authors
#distinctAuthors = df.groupBy("author").agg(countDistinct("author"))
#distinctAuthors.show()