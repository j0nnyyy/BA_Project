
from pyspark.sql.functions import col, window, avg, lit, when
from pyspark.sql import Window

def join_by_column(df1, df2, column):
    df1 = df1.withColumn(column + "1", col(column)).drop(col(column))
    df2 = df2.withColumn(column + "2", col(column)).drop(col(column))
    df_joined = df1.join(df2, col(column + "1") == col(column + "2")).select("*", col(column + "1").alias(column))\
        .drop(column + "1", column + "2")
    return df_joined

def single_revision_hotspots_by_time(df, weeks_per_bin=4, multiplier=4):
    df_windowed = df.groupBy(col("title"), window("timestamp", str(weeks_per_bin) + " weeks")).count()
    df_avg = df_windowed.groupBy(col("title")).avg()
    df_joined = join_by_column(df_windowed, df_avg, "title")
    df_hotspots = df_joined.where(col("count") >= (multiplier * col("avg(count)")))
    return df_hotspots

def global_revision_hotspots_by_time(df, weeks_per_bin=4, multiplier=4):
    df_windowed = df.groupBy(window("timestamp", str(weeks_per_bin) + " weeks")).count()
    df_avg = df_windowed.select(avg("count"))
    df_windowed = df_windowed.withColumn("avg", lit(float(df_avg.first()["avg(count)"])))
    df_windowed.show()
    df_hotspots = df_windowed.where(col("count") >= (multiplier * col("avg")))
    return df_hotspots

def sliding_window_hotspots_by_time(df, window_size="2", check_before=2, check_after=2, multiplier=2, type="title"):
    df_s_windowed = df.groupBy(col(type), window(col("timestamp"), str(window_size) + " weeks")).count()
    df_s_windowed.cache()
    df_s_windowed.show()
    windowspec = Window.partitionBy(col(type)).orderBy(col("window")).rowsBetween(-check_before, check_after)

    print("calculate moving average of revisions (-" + str(check_before) + ", " + str(check_after) + ")")
    df_s_avg = df_s_windowed.withColumn("moving_avg", avg(col("count")).over(windowspec))
    df_s_avg.show()

    #title|window|hotspot|count
    df_with_hotspots = df_s_avg.withColumn("hotspot", when(col("count") > multiplier * col("moving_avg"), 1).otherwise(0))\
        .select("hotspot", "window", type, col("count").alias("rev_count"))
    df_with_hotspots.show()
    #title|window|rev_count
    df_hotspots = df_with_hotspots.where(col("hotspot") == 1)\
        .select(type, "window", "rev_count")
    return df_hotspots

def sliding_window_category_hotspots(df, check_before=2, check_after=2, multiplier=2):
    df_windowed = df.groupBy(col("category"), window(col("timestamp"), "2 weeks")).count()
    df_windowed.cache()
    df_windowed.show()
    windowspec = Window.partitionBy(col("category")).orderBy(col("window")).rowsBetween(-check_before, check_after)

    print("calculate moving avg of category revisions")
    df_avg = df_windowed.withColumn("moving_avg", avg(col("count")).over(windowspec))
    df_avg.show()

    #category|window|hotspot|count
    df_with_hotspots = df_avg.withColumn("hotspot", when(col("count") > multiplier * col("moving_avg"), 1).otherwise(0))\
        .select("hotspot", "window", "category", col("count").alias("rev_count"))
    df_with_hotspots.show()
    #category|window|rev_count
    df_hotspots = df_with_hotspots.where(col("hotspot") == 1)\
        .select("category", "window", "rev_count")
    return df_hotspots
