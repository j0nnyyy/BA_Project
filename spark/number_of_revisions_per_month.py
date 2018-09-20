from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col, months_between, add_months
import load_to_spark
import datetime
from pyspark.sql.functions import min as min_, max as max_
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_gn = load_to_spark.init()
#print('Initial DF:')
#df_gn.select("id", "title", "revision").show()

df_groups = df_gn.select("title").distinct()
#print("Unique article titles:")
#df_groups.show()

df = load_to_spark.main_init_df()

df_monthly_ts = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))
df_monthly_ts = df_monthly_ts.groupBy("yearmonth", "title").count().orderBy(desc("count"))

df = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))
df_monthly = df.groupBy("yearmonth", "title").count().orderBy(desc("count"))
print("Number of edits per month over all articles: ")
df_monthly.select("title", "yearmonth", "count").show()

min_date, max_date = df_monthly_ts.select(min_("yearmonth").cast("long"), max_("yearmonth").cast("long")).first()

data = [(min_date, max_date)]
df_dates = spark.createDataFrame(data, ["minDate", "maxDate"])
df_min_max_date = df_dates.withColumn("minDate", col("minDate").cast("timestamp")).withColumn("maxDate", col("maxDate").cast("timestamp"))

df_formatted_ts = df_min_max_date.withColumn("monthsDiff", f.months_between("maxDate", "minDate"))\
    .withColumn("repeat", f.expr("split(repeat(',', monthsDiff), ',')"))\
    .select("*", f.posexplode("repeat").alias("date", "val"))\
    .withColumn("date", f.expr("add_months(minDate, date)"))\
    .withColumn("yearmonth", f.concat(f.year("date"), f.lit('-'), format_string("%02d", f.month("date"))))\
    .select('yearmonth')
#df_formatted_ts.show()

df_group_ts = df_groups.crossJoin(df_formatted_ts)
#print("Cross Join -> Titles - Timestamps")
#df_group_ts.show()

df_allts = df_group_ts.join(df_monthly, ['title', 'yearmonth'], how='left') \
    .orderBy('title', 'yearmonth').select('title', 'yearmonth', 'count')

df_allts.orderBy(desc('count')).show(100)

print('Calculate average edits per month for each article :')
window = Window.partitionBy("title").orderBy('yearmonth').rowsBetween(-1, 1)
df_avg = df_allts.select('title', 'yearmonth', 'count', f.round(f.avg('count').over(window), 2).alias('avg')).na.fill(0)\
    .orderBy(desc('count'))
df_avg.show(40)

print('Select only records where #edits > #average_edits :')
df_filtered = df_avg.where("count > avg")
df_filtered.show()



