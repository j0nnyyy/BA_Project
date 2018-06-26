from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import from_unixtime, size, col, udf, explode
import datetime
import load_to_spark

def convert_to_timestamp(date_text):
    return datetime.datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")

# number of authors per timestamp
def number_of_authors_per_timestamp(df, startDate, endDate):
    startDate = convert_to_timestamp(startDate)
    endDate = convert_to_timestamp(endDate)
    df.groupBy("author").agg({"author": "count"}).filter(col("timestamp").isin([startDate, endDate])).show()


df = load_to_spark.main_init_df()
number_of_authors_per_timestamp(df, '2001-01-21 03:00:00', '2001-06-03 23:00:00')