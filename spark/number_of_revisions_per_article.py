from pyspark.sql.functions import desc, asc
import pyspark.sql.functions as func
import load_to_spark


# number of all authors per each article
def number_of_authors_per_article(df):
    print("Length of revisions:")
    df_edits = df.groupBy("title", "author").agg(func.count("author").alias("count"))\
        .orderBy(desc("count"))
    return df_edits

def number_of_revisions_per_article(df):
    print("Length of revisions:")
    df_edits = df.groupBy("title").agg(func.count("*").alias("edit history length"))\
        .orderBy(desc("edit history length"))
    return df_edits


def revisions_per_article():
    df = load_to_spark.main_init_df()
    return number_of_revisions_per_article(df)


total_edits = revisions_per_article()
total_edits.show()
