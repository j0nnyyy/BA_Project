#changed matplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('Agg')

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col
import load_to_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import time

search_text = ['Bot', 'Bots']
filenames = ['/home/ubuntu/dumps/wiki_small_1.json']

def draw_histogram(df):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df], bins=20, color=['red'])
    axes[0, 0].set_xlabel('Jaccard Distanz')
    axes[0, 0].set_ylabel('Anzahl der Artikel')
    plt.savefig('title_jaccard_distance')


def sparse_vec(r):
    li = set(r[1])
    li = sorted(li)
    l = len(li)
    vals = [1.0 for x in range(l)]
#    return r[0], Vectors.sparse(942000, li, vals)
    return r[0], Vectors.sparse(1000000, li, vals)

df_gn = load_to_spark.main_init_df(filenames)

df_titles = df_gn.select("title", "author").where(col("author").isNotNull())

# Determine all distinct authors, exclude bots and generate IDs for each
df_all_authors = df_gn.select("author").where(col("author").isNotNull()).distinct()

# Select only Bots
df_bots = df_all_authors.where(col("author").rlike('|'.join(search_text)))

# Select all authors except bots
df_real_users = df_all_authors.subtract(df_bots)

windowSpec = W.orderBy("author")
df_authors = df_real_users.withColumn("Id", f.row_number().over(windowSpec))

print("Join both dataframes by author-column:")
df1_a = df_titles.alias("df1_a")
df2_a = df_authors.alias("df2_a")

df_joined = df1_a.join(df2_a, col('df1_a.author') == col('df2_a.author')).select('df1_a.title', 'df2_a.id')
df_joined.show(20)

# create a binary vector

dfWithFeat = df_joined.rdd.map(lambda r: (r['title'], (float(r['id'])))).groupByKey()\
    .map(lambda r: sparse_vec(r)).toDF()
df_res = dfWithFeat.select(col("_1").alias("title"), col("_2").alias("features"))
df_res.show()


mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_res)

# Feature Transformation
print("The hashed dataset where hashed values are stored in the column 'hashes':")
model.transform(df_res).show()

print("Approximately distance smaller than 0.6:")
df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 1.0, distCol="JaccardDistance")\
    .select(col("datasetA.title").alias("title"),
            col("JaccardDistance")).filter("JaccardDistance != 0").orderBy(desc("JaccardDistance"))
df_jacc_dist.show()

df_hist = df_jacc_dist.select(col("JaccardDistance"))

draw_histogram(df_hist)

print('DONE')
