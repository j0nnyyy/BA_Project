#changed matplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('Agg')

from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import desc, asc, format_string, col
import load_to_spark
from pyspark.sql.functions import min as min_, max as max_
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
from pyspark.sql import SparkSession
import time

logpath = '/home/ubuntu/BA_Project/log.txt'

#retrieve loaded file count
file_count = load_to_spark.filename.count(',') + 1

def draw_histogram(df1, df2):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df1], bins=20, color=['red'])
    axes[0, 0].set_title('Durchschnittliche Anzahl von Revisionen pro Monat')
    #changed non-ascii character to prevent errors
    axes[0, 0].set_xlabel('Laenge der Revisionen')
    axes[0, 0].set_ylabel('Anzahl der Artikeln')
    axes[0, 0].legend()
    hist(axes[0, 1], [df2], bins=20, color=['blue'])
    axes[0, 1].set_title('Anzahl von Revisionen pro Monat')
    #changed non-ascii character to prevent errors
    axes[0, 1].set_xlabel('Laenge der Revisionen')
    axes[0, 1].set_ylabel('Anzahl der Artikeln')
    axes[0, 1].legend()
    plt.savefig('/home/ubuntu/Average_Number_Of_Revisions_per_Month')


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.executor.memory", "128g") \
    .getOrCreate()

#get start time
start_time = time.time()

df_gn = load_to_spark.init()

#retrieve spark worker count
worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1

df_groups = df_gn.select("title").distinct()

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

df_group_ts = df_groups.crossJoin(df_formatted_ts)

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

df_avg_hist = df_filtered.select(col("avg").alias('Average edits per month')).orderBy(desc('Average edits per month'))
df_avg_hist.cache()
df_avg_hist.show()
print('count =', df_avg_hist.count())
df_count_hist = df_filtered.select(col("count").alias('Total edits per month'))

#get end time
end_time = time.time()

draw_histogram(df_avg_hist, df_count_hist)

#calculate duration and write the application information to the log file
duration = end_time - start_time
file = open(logpath, 'a+')
file.write(worker_count, file_count, duration)

print('DONE')


