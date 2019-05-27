from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

filenames = []
schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])

for i in range(1, 27):
    path = "/home/ubuntu/dumps/wiki_small_" + str(i) + ".json"
    filenames.append(path)

spark = SparkSession.builder.appName("authors per article") \
    .config("spark.executor.memory", "128g") \
    .getOrCreate()
df = spark.read.json(filenames, schema=schema)
df = df.withColumn("revision", f.explode(df.revision))
print(df.count())

#row_avg = df.select(f.avg(f.size(df.revision)).alias("avg")).first()
#avg = int(row_avg["avg"])
#row_stddev = df.select(f.stddev(f.size(df.revision)).alias("stddev")).first()
#stddev = int(row_stddev["stddev"])

#output = "avg: {}, stddev: {}".format(avg, stddev)
#print(output)
