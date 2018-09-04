from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, datediff, explode, months_between, col
import load_to_spark
import datetime
import pyspark.sql.types
from pyspark.sql.functions import udf
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = load_to_spark.main_init_df()
print("Number of revisions per time period over all articles")

df = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))
#df_yearmonth = df.groupBy("yearmonth", "title").count().orderBy(asc("yearmonth"))
df_yearmonth = df.groupBy("yearmonth", "title").count().orderBy(desc("count"))
df_yearmonth.show()

#df = df.withColumn("timestampGMT", df.editTime.cast("timestamp"))
#df = df.withColumn("count_per_timeperiod", f.count("title").over(Window.partitionBy(f.window("editTime", "7 days"))))
#df.show()
#tumblingWindowDS = df_2001.groupBy(window(col("editTime"), "1 week"))
#tumblingWindowDS.show()
#df_2001.show()


#df = df.withColumn("yearmonth", func.concat(func.year("editTime"), func.lit('-'), func.month("editTime")))
#df_agg = df.groupBy("yearmonth", "title").count()
#df_agg.show()


step = 31*60*60*24

min_date, max_date = df.select(min_("yearmonth").cast("long"), max_("yearmonth").cast("long")).first()

reference = spark.range(
    (min_date / step) * step, ((max_date / step) + 1) * step, step)\
    .select(col("id").cast("timestamp").alias("yearmonth"))
#reference.show()


df_res = reference.join(df_yearmonth, ["yearmonth"], "leftouter")\
    #.select("yearmonth",
           #f.when(col("title").isNull, f.lit(0)).otherwise(df_yearmonth("title").alias("title")),
            #f.when(col("count").isNull, f.lit(0)).otherwise(df_yearmonth("count")).alias("count"))
df_res = df_res.orderBy(asc("yearmonth"))
df_res.show()

'''

def generate_date_series(start, stop):
    return [start + datetime.timedelta(days=x) for x in range(0, (stop-start).days + 1)]
#spark.udf.register("generate_date_series", generate_date_series, ArrayType(DateType()))

w = Window.orderBy("yearmonth")
tempDf = df_yearmonth.withColumn("diff", datediff(f.lead(col("yearmonth"), 1).over(w), col("yearmonth")))\
    .filter(col("diff") > 31)\
    .withColumn("count", f.lit("0"))\
    .withColumn("yearmonth", explode(generate_date_series(col("yearmonth"), f.lead(col("yearmonth"), 1).over(w))))\
    .withColumn("yearmonth", col("yearmonth").cast(TimestampType()))


tempDf.show()
#result = df_yearmonth.join(tempDf.select("yearmonth", "count")).orderBy("date")
'''