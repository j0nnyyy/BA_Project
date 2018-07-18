from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import date_add, date_sub, desc, format_string, to_timestamp, expr
from pyspark.sql.types import TimestampType
import load_to_spark

df = load_to_spark.main_init_df()
print("Number of revisions per day over all articles")

df = df.withColumn("date", f.concat(f.year("editTime"), f.lit('-'),
                                         format_string("%02d", f.month("editTime")), f.lit('-'),
                                         format_string("%02d", f.dayofmonth("editTime"))))
df_day = df.groupBy("date", "title").count().orderBy(desc("count")).limit(1)
df_day.show()


# select also from day before and day after

df2 = df_day.select(to_timestamp("date", 'yyyy-MM-dd').alias("day"))

df = df2.withColumn('day_before', date_sub('day', 1)).withColumn('day_after', date_add('day', 1))
df.show()


window = (Window().partitionBy(col("title"))
          .orderBy(col("editTIme").cast("timestamp").cast("long")).rowsBetween(-1,1))

df = df.withColumn("occurrences", f.count("author").over(window))
df.show()
df.orderBy(desc("occurrences")).show()