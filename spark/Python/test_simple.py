from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("test").config("spark.executor.memory", "128g")\
    .config("spark.speculation", "true")\
    .config("spark.sql.shuffle.partitions", 2)\
    .getOrCreate()
sc = spark.sparkContext

df = sc.parallelize([
    Row(author="A", title="X"),
    Row(author="A", title="Y"),
    Row(author="A", title="Y"),
    Row(author="A", title="Y"),
    Row(author="B", title="Y"),
    Row(author="B", title="Z"),
    Row(author="B", title="Z"),
    Row(author="C", title="X"),
    Row(author="C", title="X"),
    Row(author="C", title="X"),
]).toDF()
df = df.distinct()

print("Self joining")
df1 = df.select(col("author").alias("author1"), col("title").alias("title1"))
df2 = df.select(col("author").alias("author2"), col("title").alias("title2"))
df_joined = df1.crossJoin(df2)
df_joined = df_joined.where(col("author1") != col("author2"))
df_joined.show()

print("df_all")
df_all = df_joined.groupBy(col("author1"), col("author2")).count()\
    .select(col("author1").alias("ad1"), col("author2").alias("ad2"), col("count").alias("dis")).distinct()
df_all.show()

print("df_common")
df_common = df_joined.where(col("title1") == col("title2")).groupBy(col("author1"), col("author2")).count()\
    .select(col("author1").alias("ac1"), col("author2").alias("ac2"), col("count").alias("con")).distinct()
df_common.show()

print("df_rest")
df_rest = df_joined.where(col("title1") != col("title2")).groupBy(col("author1"), col("author2")).count()\
    .select(col("author1").alias("ac1"), col("author2").alias("ac2"))\
    .withColumn("con", f.lit(0))
df_rest.show()

print("union common and rest")
df_common = df_common.union(df_rest).groupBy(col("ac1"), col("ac2")).sum()
df_common = df_common.select(col("ac1"), col("ac2"), col("sum(con)").alias("con"))
df_common.show()

print("Joining over both authors")
df_all = df_all.join(df_common, (col("ad1") == col("ac1")) & (col("ad2") == col("ac2")))
df_all.show()

print("selecting needed columns")
df_all = df_all.select(col("ad1"), col("ad2"), col("dis"), col("con"))
df_all.show()

print("Subtracting duplicates")
df_all = df_all.withColumn("dis", col("dis") - col("con"))
df_all.show()

print("Calculating jaccard")
df_jaccard = df_all.withColumn("jaccard", col("con") / col("dis"))
df_jaccard.show()
