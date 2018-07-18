from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, format_string
import load_to_spark

df = load_to_spark.main_init_df()
print("Number of revisions per time period over all articles")

df = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))
df_yearmonth = df.groupBy("yearmonth", "title").count().orderBy(desc("yearmonth"))
df_yearmonth.show()


#df = df.withColumn("timestampGMT", df.editTime.cast("timestamp"))
#df = df.withColumn("count_per_timeperiod", f.count("title").over(Window.partitionBy(f.window("editTime", "7 days"))))
#df.show()
#tumblingWindowDS = df_2001.groupBy(window(col("editTime"), "1 week"))
#tumblingWindowDS.show()
#df_2001.show()

'''
window = (Window().partitionBy(col("title"))
          .orderBy(col("editTIme").cast("timestamp").cast("long")).rangeBetween(-days(30), 0))

df = df.withColumn("monthly_occurrences", func.count("author").over(window))
df.show()
df.orderBy(desc("monthly_occurrences")).show()

'''

#df = df.withColumn("yearmonth", func.concat(func.year("editTime"), func.lit('-'), func.month("editTime")))
#df_agg = df.groupBy("yearmonth", "title").count()
#df_agg.show()
