from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, explode
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f

plotpath = '/home/ubuntu/article_'

def jaccard_with_crossjoin(df_t_user, to_compare, regarding, mode="dist", minval=0.0, maxval=1.0):
    df_t_user.count()
    #add counts
    df_count = df_t_user.groupBy(col(to_compare)).count().select(col(to_compare).alias(to_compare + "1"), col("count"))
    df_t_user = df_t_user.join(df_count, col(to_compare) == col(to_compare + "1"))\
        .select(col(to_compare), col(regarding), col("count"))
    df_t_user.count()

    #crossjoin to get all possible pairs
    print("Self joining...")
    df1 = df_t_user.select(col(to_compare).alias(to_compare + "1"), col(regarding).alias(regarding + "1"), col("count").alias("count1"))
    df2 = df_t_user.select(col(to_compare).alias(to_compare + "2"), col(regarding).alias(regarding + "2"), col("count").alias("count2"))
    df_joined = df1.crossJoin(df2)
    df_joined.show()
    df_joined = df_joined.where(col(to_compare + "1") < col(to_compare + "2"))
    df_joined.cache()
    df_joined = df_joined.repartition(200, col(to_compare + "1"), col(to_compare + "2"))
    df_joined.show() #show 2
    print("Join complete")

    print("Calculating all", regarding, "per", to_compare, "pair")
    df_all = df_joined.withColumn("dis", col("count1") + col("count2"))\
        .select(col(to_compare + "1").alias("d1"), col(to_compare + "2").alias("d2"), col("dis")).distinct()
    df_all.show()
    print("df_all", df_all.count())
    print("Calculating common authors")
    df_common = df_joined.where(col(regarding + "1") == col(regarding + "2")).groupBy(col(to_compare + "1"), col(to_compare + "2")).count()\
        .select(col(to_compare + "1").alias("c1"), col(to_compare + "2").alias("c2"), col("count").alias("con")).distinct()
    df_common.show()
    print("df_common", df_common.count())
    print("Calculating rest")
    df_rest = df_joined.where(col(regarding + "1") != col(regarding + "2")).groupBy(col(to_compare + "1"), col(to_compare + "2")).count()\
        .select(col(to_compare + "1").alias("c1"), col(to_compare + "2").alias("c2"))\
        .withColumn("con", f.lit(0))
    df_rest.show()
    print("df_rest", df_rest.count())
    print("Union common and rest")
    df_common = df_common.union(df_rest).groupBy(col("c1"), col("c2")).sum()
    df_common = df_common.select(col("c1"), col("c2"), col("sum(con)").alias("con"))
    df_common.show()
    print("df_common", df_common.count())
    print("Joining over both " + to_compare)
    df_all = df_all.join(df_common, (col("d1") == col("c1")) & (col("d2") == col("c2")))\
        .select(col("d1"), col("d2"), col("dis"), col("con"))
    df_all.show()
    print("df_all", df_all.count())
    print("Subtracting duplicates")
    df_all = df_all.withColumn("dis", col("dis") - col("con"))
    df_all.show()
    print("df_all - no duplicates", df_all.count())

    #calculate jaccard
    print("Calculating jaccard")
    if mode == 'sim':
        df_jaccard = df_all.withColumn("jaccard", col("con") / col("dis"))
    elif mode == 'dist':
        df_jaccard = df_all.withColumn("jaccard", 1.0 - (col("con") / col("dis")))
    print("df_jaccard", df_jaccard.count())

    df_jaccard = df_jaccard.select(col("d1").alias(to_compare + "1"), col("d2").alias(to_compare + "2"), col("jaccard"))
    df_jaccard = df_jaccard.where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    df_jaccard.show()

    return df_jaccard

def jaccard_with_min_hashing(df_t_user, to_compare, regarding, mode="dist", minval=0.0, maxval=1.0):
    #get regarding
    df_regarding = df_t_user.select(col(regarding)).distinct()

    #create ids for each regarding element
    print("Creating ids")
    windowSpec = W.orderBy(regarding)
    df_regarding = df_regarding.withColumn("id", f.row_number().over(windowSpec))

    #window function moved df_titles to single partition --> repartition
    df_regarding.repartition(200)
    print("df_regarding", df_regarding.count())

    #join dataframes to get author/id pairs
    print("Joining...")
    df1 = df_t_user.alias("df1")
    df2 = df_regarding.alias("df2")
    df_joined = df1.join(df2, col('df1.' + regarding) == col('df2.' + regarding)).select(col('df1.' + to_compare), col('df2.id'))
    df_joined.show()
    print("df_joined", df_joined.count())
    print("Join Complete")

    #create binary vectors
    print("Creating vectors")
    count = df_regarding.count() + 10
    max_index = int(df_regarding.select(col("id")).orderBy(desc("id")).first()["id"])
    size = max(count, max_index)
    df_joined = df_joined.rdd.map(lambda r: (r[to_compare], float(r['id']))).groupByKey().map(lambda r: sparse_vec(r, size)).toDF()
    print("df_joined", df_joined.count())

    df_res = df_joined.select(col('_1').alias(to_compare), col('_2').alias('features'))
    df_res.show()
    df_res = df_res.repartition(200)
    print("df_res", df_res.count())

    print("Creating model")
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=200)
    model = mh.fit(df_res)
    model.transform(df_res).show()

    print("Calculating Jaccard")
    df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 1.0, distCol="jaccard")
    df_jacc_dist.show()
    print("df_jacc_dist", df_jacc_dist.count())

    print("Selecting needed columns")
    df_jacc_dist = df_jacc_dist.select(col("datasetA." + to_compare).alias(to_compare + "1"),
                col("datasetB." + to_compare).alias(to_compare + "2"),
                col("jaccard")).where(col(to_compare + "1") < col(to_compare + "2"))
    print(df_jacc_dist.count())
    df_jacc_dist = df_jacc_dist.where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    print(df_jacc_dist.count())

    if mode == "sim":
        df_jacc_dist = df_jacc_dist.withColumn("jaccard", 1.0 - col("jaccard"))

    return df_jacc_dist

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    ones = [1.0 for i in range(length)]
    return r[0], Vectors.sparse(count, list, ones)
