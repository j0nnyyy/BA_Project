from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

spark = SparkSession.builder.appName("test").config("spark.executor.memory", "128g").getOrCreate()

data = [(Vectors.sparse(4,[(0,1.0)]),),
        (Vectors.sparse(4,[(0,1.0)]),),
        (Vectors.sparse(4,[(1,1.0)]),),
        (Vectors.sparse(4,[(1,1.0)]),),
        (Vectors.sparse(4,[(2,1.0)]),),
        (Vectors.sparse(4,[(2,1.0)]),),
        (Vectors.sparse(4,[(3,1.0)]),),
        (Vectors.sparse(4,[(3,1.0)]),)]

#data = [(Vectors.sparse(8,[(0,1.0),(1,1.0)]),),
#        (Vectors.sparse(8,[(0,1.0),(3,1.0),(4,1.0)]),),
#        (Vectors.sparse(8,[(4,1.0),(5,1.0)]),),
#        (Vectors.sparse(8,[(6,1.0),(7,1.0)]),)]

df = spark.createDataFrame(data, ["features"])
corr = Correlation.corr(df, "features").head()
print(str(corr[0]))
