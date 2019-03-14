#changed matplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('Agg')

import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import desc, col
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import load_to_spark


def draw_histogram(df1, df2, df3):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df1], bins=20, color=['red'])
    axes[0, 0].set_title('Anzahl von Revisionen pro Monat')
    axes[0, 0].set_xlabel('Anzahl der Revisionen')
    axes[0, 0].set_ylabel('Anzahl der Artikeln')
    hist(axes[0, 1], [df2], bins=20, color=['blue'])
    axes[0, 1].set_title('Anzahl von Revisionen über Autoren pro Monat')
    axes[0, 1].set_xlabel('Anzahl der Revisionen pro Autor')
    axes[0, 1].set_ylabel('Anzahl der Artikeln')
    hist(axes[1, 0], [df3], bins=20, color=['tan'])
    axes[1, 0].set_title('Variance')
    axes[1, 0].set_xlabel('Variance')
    axes[1, 0].set_ylabel('Anzahl der Artikeln')
    plt.savefig('NumberOfRevisionsPerMonth_AuthorRevisionsPerMonth')


def compute_variance(df):
    df_res = df.groupBy("title").agg(f.round(f.var_pop("count"), 2).alias("variance"))
    return df_res


# total number of revisions per article
def number_of_revisions_per_article(df):
    df_total_revisions = df.groupBy("title").agg(f.count("*").alias("total revisions"))\
        .orderBy(desc("total revisions"))
    return df_total_revisions


df = load_to_spark.main_init_df()

print("Number of edits per authors per month:")
df_author = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'),
                                                format_string("%02d", f.month("editTime"))))
df_authors_per_month = df_author.groupBy("yearmonth", "author").count().orderBy(desc("yearmonth"))
df_authors_per_month.cache()
df_authors_per_month.show()


print("Number of pages per month:")
df_page = df.withColumn("yearmonth", f.concat(f.year("editTime"), f.lit('-'), format_string("%02d", f.month("editTime"))))
df_pages_per_month = df_page.groupBy("yearmonth", "title").count().orderBy(desc("count"))
df_pages_per_month.cache()
df_pages_per_month.show()

print("Number of revisions per article:")
df_all_revisions = number_of_revisions_per_article(df)
df_all_revisions.cache()
df_all_revisions.show()


df_joined = df_pages_per_month.join(df_all_revisions, ['title'], how='outer')\
    .orderBy('title', 'yearmonth')\
    .select('yearmonth', 'title', 'count', 'total revisions')
df_joined.cache()
df_joined.orderBy(desc("count")).show(20)

df_variance = compute_variance(df_joined)
df_variance.cache()
df_variance.orderBy(desc("variance")).show(20)
print('Count variance = ', df_variance.count())

# Draw histograms
df_authors_per_month_hist = df_authors_per_month.select(col("count")).alias("Edits of users per month")
df_pages_per_month_hist = df_pages_per_month.select(col("count")).alias('Edits per month for each article')
df_variance_hist = df_variance.select(col("variance")).alias('Variance overall statistics')
draw_histogram(df_pages_per_month_hist, df_authors_per_month_hist, df_variance_hist)

print('DONE')