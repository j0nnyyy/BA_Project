from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number, col, desc
import number_of_edits_per_article
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, desc
import pyspark.sql.functions as func
import load_to_spark

NUM = 10

def top_active_authors_per_article(df, NUM):
    window = Window.partitionBy("title").orderBy(desc("count"))

    df.select('*', row_number().over(window).alias('rank'))\
        .filter(col('rank') <= NUM).show()

df = load_to_spark.main_init_df()
#df = number_of_edits_per_article.main_length_of_revisions()
#df.show()

top_active_authors_per_article(df, NUM)

