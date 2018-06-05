from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, countDistinct

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.load("wiki.json", format="json")

# extract fields from revisions
df = df.withColumn("author", df["revision"].getItem(0).contributor.username)\
    .withColumn("authorID", df["revision"].getItem(0).contributor.id)\
    .withColumn("date", df["revision"].getItem(0).timestamp)

#df.select("articleID", "article","revision", "author", "authorID", "date").show()
df.show()

# number of edits per author
print("Number of edits per author")
authors = df.groupBy("author").count().orderBy("count", ascending=False)
authors.show()

# get length of edit per article
print("Size of edit per article")
edit_len = df.select(size(df.revision)).collect()
print(edit_len)

# number of unique authors
distinctAuthors = df.groupBy("author").agg(countDistinct("author"))
distinctAuthors.show()

#df.printSchema()
