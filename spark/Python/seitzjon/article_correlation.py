from pyspark.sql.functions import col, window, lit, lag, row_number, when
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import Vectors
from pyspark.sql import Window, Row
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
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
    df = load_to_spark.get_samples(filenames, 1000).select("author", "title", "id", col("editTime").alias("timestamp"))
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
    df_titles = df.select("title").distinct()
    windowspec = Window.orderBy("title")
    df_ids = df_titles.withColumn("id", row_number().over(windowspec))
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
    plt.ylabel("Anzahl der Titelpaare")
    plt.hist(list, bins, color="red")
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/article_hotspot_correlation.png")

def draw_histograms(df1, df2, df3):
    plt.rcParams.update({'font.size': 14})
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    axes[0, 0].set_xlabel("Jaccard-Distanz der Kategorien negativ korrelierender Artikel")
    axes[0, 0].set_ylabel("Anzahl der Artikelpaare")
    if not df1.rdd.isEmpty():
        hist(axes[0, 0], [df1], color=["red"])
    axes[0, 1].set_xlabel("Jaccard-Distanz der Kategorien positiv korrelierender Artikel")
    axes[0, 1].set_ylabel("Anzahl der Artikelpaare")
    if not df2.rdd.isEmpty():
        hist(axes[0, 1], [df2], color=["red"])
    axes[1, 0].set_xlabel("Jaccard-Distanz der Kategorien wenig korrelierender Artikel")
    axes[1, 0].set_ylabel("Anzahl der Artikelpaare")
    if not df3.rdd.isEmpty():
        hist(axes[1, 0], [df3], color=["red"])
    plt.savefig("/scratch/wikipedia-dump/plots/jaccard/category_similarity_per_correlating_article.png")

df = init_df(filenames)

df_needed = df.select("author", "title", "timestamp")
df_windowed = window_df(df_needed)
#window
df_windows = df_windowed.select(col("window")).distinct()

#title
df_titles = df.select(col("title")).distinct()

df_all = df_windowed.groupBy("title", "window").count()

#handle trend with 'first differences'
windowspec = Window.partitionBy(col("title")).orderBy(col("window"))
df_no_trend = df_all.withColumn("previous", lag(col("count")).over(windowspec))
df_no_trend = df_no_trend.select("title", "window", when(col("previous").isNotNull(), col("count") - col("previous")).otherwise(lit(0)).alias("count"))
df_no_trend.show()

df_ids, count = create_ids(df_titles)
print("df_ids", df_ids.count())
df_joined = df_ids.join(df_no_trend, "title")

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

df_category = init_category_df()
df_t_id = df.select("id", "title").distinct()
df_c_id = df_category.join(df_t_id, "id").select("title", "category")
df_c_id = df_c_id.join(df_ids, "title").select("id", "category")

df_negative_corr = df_corr.where(col("pearson") == "negative").select("id1", "id2")
df_positive_corr = df_corr.where(col("pearson") == "positive").select("id1", "id2")
df_rest_ids = df_corr.select("id1", "id2").subtract(df_positive_corr).subtract(df_negative_corr)
df_rest_ids.show()

df_ids = df_negative_corr.select(col("id1").alias("id")).union(df_negative_corr.select(col("id2").alias("id"))).distinct()
df_neg = df_c_id.join(df_ids, "id").select("category", col("id").alias("to_compare"))
df_neg_jacc = jaccard_similarity.jaccard_with_min_hashing(df_neg, "to_compare", "category")
df_neg_jacc = df_neg_jacc.join(df_negative_corr, (col("id1") == col("to_compare1")) & (col("id2") == col("to_compare2")))
df_neg_jacc.show()

df_ids = df_positive_corr.select(col("id1").alias("id")).union(df_positive_corr.select(col("id2").alias("id"))).distinct()
df_pos = df_c_id.join(df_ids, "id").select("category", col("id").alias("to_compare"))
df_pos_jacc = jaccard_similarity.jaccard_with_min_hashing(df_pos, "to_compare", "category")
df_pos_jacc = df_pos_jacc.join(df_positive_corr, (col("id1") == col("to_compare1")) & (col("id2") == col("to_compare2")))
df_pos_jacc.show()

print("Rest")
df_ids = df_rest_ids.select(col("id1").alias("id")).union(df_rest_ids.select(col("id2").alias("id"))).distinct()
df_ids.show()
df_rest = df_c_id.join(df_ids, "id").select("category", col("id").alias("to_compare"))
df_rest.show()
df_rest_jacc = jaccard_similarity.jaccard_with_min_hashing(df_rest, "to_compare", "category")
df_rest_jacc = df_rest_jacc.join(df_rest_ids, (col("id1") == col("to_compare1")) & (col("id2") == col("to_compare2")))

df_n_hist = df_neg_jacc.select("jaccard")
df_p_hist = df_pos_jacc.select("jaccard")
df_r_hist = df_rest_jacc.select("jaccard")

draw_histograms(df_n_hist, df_p_hist, df_r_hist)
