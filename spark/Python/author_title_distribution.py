from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pyspark.sql.functions as f
import load_to_spark

base_path = "/home/ubuntu/dumps/wiki_small_"
log_path = "/home/ubuntu/BA_Project/log/author_title_log.txt"
bots = ['bot', 'bots']

schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])

spark = SparkSession.builder.appName("Author-title-distribution").config("spark.executor.memory", "128g").getOrCreate()
file = open(log_path, "+a")

for i in range(1, 27):
    path = base_path + str(i) + ".json"
    #get title count
    df = spark.read.json(path, schema)
    title_count = df.count()
    print(i, "title")
    #get avg/stddev for revisions per article
    avg_rev_per_art = int(df.select(f.avg(f.size(df.revision)) \
        .alias("avg")).first()["avg"])
    print(i, "avg")
    stddev_rev_per_art = int(df.select(f.stddev(f.size(df.revision)) \
        .alias("stddev")).first()["stddev"])
    print(i, "stddev")

    #get total revision count
    df = load_to_spark.extract_df_from_revisions(df)
    revision_count = df.count()
    print(i, "revisions")

    df = df.select("author").distinct()
    df = df.where(df.author.isNotNull())
    df_bots = df.where(df.author.rlike("|".join(bots)))
    df_users = df.subtract(df_bots)
    #get author count
    author_count = df_users.count()
    print(i, "authors")

    output = "{} {} {} {} {} {}\n".format(i, author_count, title_count, revision_count, avg_rev_per_art, stddev_rev_per_art)
    file.write(output)
    print("Done with", i)

file.close()
