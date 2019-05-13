from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, monotonically_increasing_id, explode
import load_to_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import time

filenames = ['/scratch/wikipedia-dump/wiki_small_1.json']
bot_names = ['Bot', 'Bots']
schema = StructType([StructField("id",StringType(),True),StructField("revision", \
    ArrayType(StructType([StructField("comment",StringType(),True),StructField("contributor", \
    StructType([StructField("id",StringType(),True),StructField("ip",StringType(),True), \
    StructField("username",StringType(),True)]),True),StructField("id",StringType(),True), \
    StructField("parentid",StringType(),True),StructField("timestamp",StringType(),True)]),True), \
    True),StructField("title",StringType(),True)])

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    ones = [1.0 for i in range(length)]
    return r[0], Vectors.sparse(count, list, ones)
    
def draw_histogram(df):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df], bins=20, color=['red'])
    axes[0, 0].set_xlabel('Jaccard Koeffizient')
    axes[0, 0].set_ylabel('Anzahl der Autoren')
    plt.savefig('Jaccard_Similarity')

    
df = load_to_spark.main_init_df()

#get all title/author pairs
df_t_a = df.select(df.title, df.author).where(col('author').isNotNull())
df_t_bot = df_t_a.where(df.author.rlike('|'.join(bot_names)))
df_t_user = df_t_a.subtract(df_t_bot)

#get titles
df_titles = df.select(df.title).distinct()

#create ids for each title
windowSpec = W.orderBy("title")
df_titles = df_titles.withColumn("id", f.row_number().over(windowSpec))

#join dataframes to get author/id pairs
df1 = df_t_user.alias("df1")
df2 = df_titles.alias("df2")
df_joined = df1.join(df2, col('df1.title') == col('df2.title')).select(col('df1.author'), col('df2.id'))

#create binary vectors
count = df_joined.count()
df_joined = df_joined.rdd.map(lambda r: (r['author'], float(r['id']))).groupByKey().map(lambda r: sparse_vec(r, count)).toDF()

df_res = df_joined.select(col('_1').alias('author'), col('_2').alias('features'))
df_res.show()

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_res)

model.transform(df_res).show()

df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 0.6, distCol="JaccardDistance")\
    .select(col("datasetA.author").alias("author1"),
            col("datasetB.author").alias("author2"),
            col("JaccardDistance")).filter("JaccardDistance != 0").orderBy(desc("JaccardDistance"))
df_jacc_dist.show()

df_hist = df_jacc_dist.select(col("JaccardDistance"))

draw_histogram(df_hist)