from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import from_unixtime, size, col, udf, explode
import datetime
import load_to_spark


# number of all authors per each article
def number_of_authors_per_article(df):
    df.groupBy("title", "author").agg({"author": "count"}).alias("count").show()


df = load_to_spark.main_init_df()
number_of_authors_per_article(df)