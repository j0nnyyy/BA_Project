from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col, explode, collect_set
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
    df_joined = df_joined.where(col(to_compare + "1") < col(to_compare + "2"))
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
    df_t_user = df_t_user.distinct()
    #get regarding
    df_regarding = df_t_user.select(col(regarding)).distinct()
    print("regarding", df_regarding.count())

    if df_regarding == None or df_regarding.rdd.isEmpty():
        return None

    #create ids for each regarding element
    print("Creating ids")
    windowSpec = W.orderBy(regarding)
    df_regarding = df_regarding.withColumn("id", f.row_number().over(windowSpec))
    df_regarding.groupBy("id").count().orderBy(desc("count")).show()

    #window function moved df_titles to single partition --> repartition
    df_regarding.repartition(200)
    df_regarding.show()

    #join dataframes to get author/id pairs
    print("Joining...")
    df1 = df_t_user.alias("df1")
    df2 = df_regarding.alias("df2")
    df_joined = df1.join(df2, col('df1.' + regarding) == col('df2.' + regarding)).select(col('df1.' + to_compare).alias(to_compare), col('df2.id').alias("id"))
    df_joined.show()
    print("Join Complete")

    #create binary vectors
    print("Creating vectors")
    count = df_regarding.count() + 10
    tmp = df_regarding.select(col("id")).orderBy(desc("id")).first()
    print("max_id", tmp["id"])
    if tmp != None:
        max_index = int(tmp["id"]) + 10
    else:
        max_index = 0
    size = max(count, max_index)
    #df_joined = df_joined.rdd.map(lambda r: (r[to_compare], float(r['id']))).groupByKey().map(lambda r: sparse_vec(r, size)).toDF()
    df_joined = df_joined.groupBy(to_compare).agg(collect_set("id")).rdd.map(lambda r: sparse_vec(r, size)).toDF()
    print("df_joined", df_joined.count())

    df_res = df_joined.select(col('_1').alias(to_compare), col('_2').alias('features'))
    df_res.show()
    df_res = df_res.repartition(200)
    #df_res.cache()
    print("df_res", df_res.count())

    print("Creating model")
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=100)
    model = mh.fit(df_res)
    model.transform(df_res).show()

    print("Calculating Jaccard")
    df_jacc_dist = model.approxSimilarityJoin(df_res, df_res, 1.0, distCol="jaccard")
    df_jacc_dist.cache()
    df_jacc_dist.show()

    print("Selecting needed columns")
    df_filtered = df_jacc_dist.select(col("datasetA." + to_compare).alias(to_compare + "1"),
                col("datasetB." + to_compare).alias(to_compare + "2"),
                col("jaccard"))
    df_filtered.show()
    df_filtered = df_filtered.where(col(to_compare + "1") < col(to_compare + "2"))
    df_filtered.show()
    #hier irgendwo Problem
    df_needed = df_filtered.where((col("jaccard") >= minval) & (col("jaccard") <= maxval))
    df_needed.show()

    if mode == "sim":
        df_needed = df_needed.withColumn("jaccard", 1.0 - col("jaccard"))

    return df_needed

#requires df1 and df2 to have the same columns
def jaccard_two_dfs(df1, df2, to_compare, regarding, minval=0.0, maxval=1.0):
    df_union = df1.union(df2)
    df_union.cache()
    df_jaccard = jaccard_with_min_hashing(df_union, to_compare, regarding)
    df_needed = df_jaccard.where((col(to_compare + "1").startswith("::C::") & (~col(to_compare + "2").startswith("::C::"))) | (col(to_compare + "2").startswith("::C::") & (~col(to_compare + "1").startswith("::C::"))))
    return df_needed

def sparse_vec(r, count):
    list = set(r[1])
    list = sorted(list)
    length = len(list)
    ones = [1.0 for i in range(length)]
    return r[0], Vectors.sparse(count, list, ones)
