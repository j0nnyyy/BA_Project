from pyspark.sql.functions import desc
import load_to_spark


# total number of edits per author
def numbder_of_revisions_per_author(df):
    print("Number of edits per author")
    authors = df.groupBy("author").count().orderBy(desc("count"))
    return authors


def revisions_per_author():
    df = load_to_spark.main_init_df()
    return numbder_of_revisions_per_author(df)


revisions_per_author().show()
