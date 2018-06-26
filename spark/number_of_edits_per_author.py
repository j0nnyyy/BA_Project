import load_to_spark


# total number of edits per author
def numbder_of_edits_per_author(df):
    print("Number of edits per author")
    authors = df.groupBy("author").count().orderBy("count", ascending=False)
    authors.show()


df = load_to_spark.main_init_df()
numbder_of_edits_per_author(df)
