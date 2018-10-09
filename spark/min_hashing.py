from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinHashLSH
from pyspark.sql.functions import desc, asc, col
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import load_to_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as f

df_gn = load_to_spark.main_init_df()


df_titles = df_gn.select("title", "author").where(col("author").isNotNull())


print("Determine all distinct authors and generate IDs for each : ")
df_all_authors = df_gn.select("author").where(col("author").isNotNull()).distinct()

print(df_all_authors.count())

windowSpec = W.orderBy("author")
df_authors = df_all_authors.withColumn("Id", f.row_number().over(windowSpec))
df_authors.show()

print("Join both dataframes by author-column:")
df1_a = df_titles.alias("df1_a")
df2_a = df_authors.alias("df2_a")

df_joined = df1_a.join(df2_a, col('df1_a.author') == col('df2_a.author')).select('df1_a.title', 'df2_a.id')
df_joined.show(20)

# create a binary vector

dfWithFeat = df_joined\
    .rdd\
    .map(lambda row: (row['id'], row['title'])).groupByKey()\
    .map(lambda row: LabeledPoint(row['title'], Vectors.sparse(1000, row['features'].toArray())))\
    .toDS


df_pivoted = df_joined.groupBy("title").pivot("id").count().na.fill(0)
df_pivoted.show(2)

input_cols = [x for x in df_pivoted.columns if x != "title"]

df_res = (VectorAssembler(inputCols=input_cols, outputCol="features").transform(df_pivoted).select("title", "features"))
df_res.show(2)

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(df_res)

# Feature Transformation
print("The hashed dataset where hashed values are stored in the column 'hashes':")
model.transform(df_res).show()

print("Approximately distance smaller than 0.6:")
model.approxSimilarityJoin(df_res, df_res, 0.6, distCol="JaccardDistance")\
    .select(col("datasetA.title").alias("idA"),
            col("JaccardDistance")).show()

