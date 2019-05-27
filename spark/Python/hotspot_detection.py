
from pyspark.sql.functions import col, window, avg, lit

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
