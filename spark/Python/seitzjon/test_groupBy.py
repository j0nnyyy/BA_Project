from pyspark.sql.functions import col, length, format_string
import pyspark.sql.functions as f
import load_to_spark
import time

base_path = "/scratch/wikipedia-dump/wiki_small_"
filenames = [base_path + str(i) + ".json" for i in range(1, 6)]

df = load_to_spark.main_init_df(filenames)
df_monthly_ts = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))\
    .withColumn("yearmonth", col("yearmonth").cast("timestamp"))
start = time.time()
df = df_monthly_ts.groupBy("yearmonth", "title").count()
df.count()
end = time.time()
print("duration", end - start)
