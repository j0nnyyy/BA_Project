from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pyspark.sql.functions as f
from pyspark.sql.functions import col
import load_to_spark

base_path = "/home/ubuntu/dumps/wiki_small_"
log_path = "/home/ubuntu/BA_Project/log/articles_per_author_log.txt"
bots = ['bot', 'bots']
schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])

for i in range(1, 27):
    logfile = open(log_path, "+a")
    path = base_path + str(i) + '.json'
    df = load_to_spark.main_init_df(path)
    df = df.drop(df.id)
    df = df.drop(df.authorID)
    df = df.where(df.author.isNotNull())
    df_bots = df.where(df.author.rlike('|'.join(bots)))
    df_users = df.subtract(df_bots)

    #title|author|timestamp --> revision count
    df_rev_per_user = df_users.groupBy(df_users.author).count()
    avg_rev = int(df_rev_per_user.select(f.avg(col("count")) \
        .alias("avg")).first()["avg"])
    stddev_rev = int(df_rev_per_user.select(f.stddev(col("count")) \
        .alias("stddev")).first()["stddev"])

    df_users = df_users.drop(df_users.editTime).distinct()

    #title|author --> article count
    df_art_per_user = df_users.groupBy(df_users.author).count()
    avg_art = int(df_art_per_user.select(f.avg(col("count")) \
        .alias("avg")).first()["avg"])
    stddev_art = int(df_art_per_user.select(f.stddev(col("count")) \
        .alias("stddev")).first()["stddev"])

    output = "{} {} {} {}\n".format(avg_rev, stddev_rev, avg_art, stddev_art)
    logfile.write(output)
    logfile.close()
    print("Done", i)

print("Done")
