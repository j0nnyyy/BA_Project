from pyspark.sql.functions import col, window, lit, lag, row_number, when, asc
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.sql import Window, Row
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
from pyspark.sql import Row
import load_to_spark
import jaccard_similarity
import numpy as np

base_path = "/scratch/wikipedia-dump/wiki_small_"
bots = ["Bot", "Bots"]
filenames = []

#for i in range(1, 27):
#    filenames.append(base_path + str(i) + ".json")

filenames.append(base_path + "1.json")

def init_df(filenames):
#    df = load_to_spark.init_article_hotspot_df(filenames)
    df = load_to_spark.get_author_samples(filenames, 1000).select("author", "title", col("authorID").alias("id"), col("editTime").alias("timestamp"))
    df = df.where(col("author").isNotNull())
    dfbots = df.where(col("author").rlike("|".join(bots)))
    df = df.subtract(dfbots)
    return df

def init_category_df():
    df = load_to_spark.create_category_df()
    return df

def window_df(df):
    df_windowed = df.withColumn("window", window(col("timestamp"), "4 weeks"))
    return df_windowed

def create_ids(df):
    df_author = df.select("author").distinct()
    windowspec = Window.orderBy("author")
    df_ids = df_author.withColumn("id", row_number().over(windowspec))
    df_ids = df_ids.withColumn("id", col("id") - 1)
    count = df_ids.count()
    return df_ids, count

def create_vectors(df, vector_length):
    df_vectors = df.rdd.map(lambda r: (r["window"], (float(r['id']), float(r['count'])))).groupByKey()\
        .map(lambda r: sparse_vec(r, vector_length)).toDF()
    return df_vectors

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    indices = [list[i][0] for i in range(length)]
    values = [list[i][1] for i in range(length)]
    print(len(indices))
    print(len(values))
    return r[0], Vectors.sparse(count, indices, values)

def draw_histogram(list):
    bins = np.linspace(-1,1,20)
    plt.xlim([-1,1])
    fig = plt.figure()
    fig.set_size_inches(20, 20)
    plt.rcParams.update({'font.size': 28})
    plt.yscale("log")
    plt.xlabel("Pearson Korrelation")
    plt.ylabel("Anzahl der Autorpaare")
    plt.hist(list, bins, color="red")
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/author_hotspot_correlation.png")

def draw_histograms(df1, df2, df3):
    plt.rcParams.update({'font.size': 14})
    plt.yscale("log")
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    axes[0, 0].set_xlabel("Jaccard-Distanz der Artikel negativ korrelierender Autoren")
    axes[0, 0].set_ylabel("Anzahl der Autorpaare")
    if not df1.rdd.isEmpty():
        hist(axes[0, 0], [df1], color=["red"])
    axes[0, 1].set_xlabel("Jaccard-Distanz der Artikel positiv korrelierender Autoren")
    axes[0, 1].set_ylabel("Anzahl der Artikelpaare")
    if not df2.rdd.isEmpty():
        hist(axes[0, 1], [df2], color=["red"])
    axes[1, 0].set_xlabel("Jaccard-Distanz der Artikel wenig korrelierender Autoren")
    axes[1, 0].set_ylabel("Anzahl der Artikelpaare")
    if not df3.rdd.isEmpty():
        hist(axes[1, 0], [df3], color=["red"])
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/article_similarity_per_author_correlation.png")

df = init_df(filenames)
df_needed = df.select("author", "title", "timestamp")
df_windowed = window_df(df_needed)
#window
df_windows = df_windowed.select(col("window")).distinct()

#title
df_authors = df.select(col("author")).distinct()

df_all = df_windowed.groupBy("author", "window").count()

#handle trend with 'first differences'
windowspec = Window.partitionBy(col("author")).orderBy(col("window"))
df_no_trend = df_all.withColumn("previous", lag(col("count")).over(windowspec))
df_no_trend = df_no_trend.select("author", "window", when(col("previous").isNotNull(), col("count") - col("previous")).otherwise(lit(0)).alias("count"))
df_no_trend.show()

df_ids, count = create_ids(df_authors)
print("df_ids", df_ids.count())
df_joined = df_ids.join(df_no_trend, "author")

df_vectors = create_vectors(df_joined, count)
df_vectors.show()
df_needed = df_vectors.select(col("_1").alias("window"), col("_2").alias("features")).select("features")
df_needed.show()

df_matrix = Correlation.corr(df_needed, "features")
corr = df_matrix.head()
tmp = corr[0].toArray()

correlations = []
rows = []

for i in range(len(tmp)):
    for j in range(i + 1, len(tmp[i])):
        if tmp[i][j] != 'nan':
            correlations.append(tmp[i][j])
            if tmp[i][j] > 0.5:
                tmpstring = "positive"
            elif tmp[i][j] < -0.5:
                tmpstring = "negative"
            else:
                tmpstring = "rest"
            rows.append(Row(id1=i, id2=j, pearson=tmpstring))

draw_histogram(correlations)

df_corr = load_to_spark.sc.parallelize(rows).toDF()
df_corr.show()

#ids von autoren mit bestimmter korrelation
df_negative_corr = df_corr.where(col("pearson") == "negative").select("id1", "id2")
print("negative_count", df_negative_corr.count())
df_positive_corr = df_corr.where(col("pearson") == "positive").select("id1", "id2")
print("positive_count", df_positive_corr.count())
df_rest_ids = df_corr.select("id1", "id2").subtract(df_positive_corr).subtract(df_negative_corr)
print("rest_count", df_rest_ids.count())
df_rest_ids.show()
df_author_title = df.select("author", "title").distinct()

#benötigt: autoren und artikel
df_needed_ids = df_negative_corr.select(col("id1").alias("id")).union(df_negative_corr.select(col("id2").alias("id"))).distinct()
df_author_id = df_ids.join(df_needed_ids, "id").distinct()
df_a_t = df_author_title.join(df_author_id, "author").select("author", "title")
df_neg_jacc = jaccard_similarity.jaccard_with_min_hashing(df_a_t, "author", "title")
df_author_pairs = df_author_id.join(df_negative_corr, col("id") == col("id1")).select(col("author").alias("a1"), col("id2")).join(df_author_id, col("id") == col("id2")).select("a1", col("author").alias("a2"))
df_hist_1 = df_neg_jacc.join(df_author_pairs, (col("author1") == col("a1")) & (col("author2") == col("a2"))).select("jaccard")
df_hist_1.show()
ones = df_author_pairs.count() - df_hist_1.count()
list = [Row(jaccard=1.0) for i in range(ones)]
df_ones = load_to_spark.sc.parallelize(list).toDF()
df_hist_1 = df_hist_1.union(df_ones)
df_hist_1.show()

df_needed_ids = df_positive_corr.select(col("id1").alias("id")).union(df_positive_corr.select(col("id2").alias("id"))).distinct()
df_author_id = df_ids.join(df_needed_ids, "id").distinct()
df_a_t = df_author_title.join(df_author_id, "author").select("author", "title")
df_pos_jacc = jaccard_similarity.jaccard_with_min_hashing(df_a_t, "author", "title")
df_author_pairs = df_author_id.join(df_positive_corr, col("id") == col("id1")).select(col("author").alias("author1"), col("id2")).join(df_author_id, col("id") == col("id2")).select("author1", col("author").alias("author2"))
df_author_pairs.show()
df_hist_2 = df_pos_jacc.join(df_author_pairs, ["author1", "author2"]).select("jaccard")
ones = df_author_pairs.count() - df_hist_2.count()
list = [Row(jaccard=1.0) for i in range(ones)]
df_ones = load_to_spark.sc.parallelize(list).toDF()
df_hist_2 = df_hist_2.union(df_ones)
df_hist_2.show()

df_needed_ids = df_rest_ids.select(col("id1").alias("id")).union(df_rest_ids.select(col("id2").alias("id"))).distinct()
df_author_id = df_ids.join(df_needed_ids, "id").distinct()
df_a_t = df_author_title.join(df_author_id, "author").select("author", "title")
df_rest_jacc = jaccard_similarity.jaccard_with_min_hashing(df_a_t, "author", "title")
df_author_pairs = df_author_id.join(df_rest_ids, col("id") == col("id1")).select(col("author").alias("author1"), col("id2")).join(df_author_id, col("id") == col("id2")).select("author1", col("author").alias("author2"))
df_hist_3 = df_rest_jacc.join(df_author_pairs, ["author1", "author2"]).select("jaccard")
ones = df_author_pairs.count() - df_hist_3.count()
list = [Row(jaccard=1.0) for i in range(ones)]
df_ones = load_to_spark.sc.parallelize(list).toDF()
df_hist_3 = df_hist_3.union(df_ones)
df_hist_3.show()

draw_histograms(df_hist_1, df_hist_2, df_hist_3)
