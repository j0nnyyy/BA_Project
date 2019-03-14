import findspark
findspark.init()

import pyspark.sql.functions as f
import load_to_spark
from pyspark.sql.functions import min, max
from pyspark.sql.functions import format_string, desc, col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("min_max_date").config("spark.executor.memory", "128g").getOrCreate()

df = load_to_spark.main_init_df_test()
#df.show()

df_monthly_ts = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("edittime")))).withColumn("yearmonth", col("yearmonth").cast("timestamp"))
df_monthly_ts = df_monthly_ts.groupby("yearmonth", "title").count().orderBy(desc("count"))#

min_date, max_date = df_monthly_ts.select(min("yearmonth").cast("long"), max("yearmonth").cast("long")).first()

print("mindate", min_date)
print("maxdate", max_date)
