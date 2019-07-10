import load_to_spark

file = "/scratch/wikipedia-dump/categorylinks.sql"

df = load_to_spark.create_category_df(file)
df.show()
