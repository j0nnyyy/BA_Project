from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
import pyspark.sql.functions as f
import jaccard_similarity

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
    Row(author="C", title="Z"),
]).toDF()
df = df.distinct()

df_res = jaccard_similarity.jaccard_with_min_hashing(df, "author", "title", mode="dist")
df_res.show()
