#change mathplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('agg')

from pyspark.sql.functions import desc, col, asc
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import load_to_spark
import time

search_text = ['Bot', 'Bots']
logpath = '/home/ubuntu/BA_Project/log.txt'

#retrieve loaded file count
file_count = len(load_to_spark.filename)

# number of all authors per each article
def number_of_authors_per_article(df):
    print("Number of revisions of authors per article:")
    df_edits = df.groupBy("title", "author").agg(f.count("author").alias("count"))\
        .orderBy(desc("count"))
    return df_edits


# total number of revisions per article
def number_of_revisions_per_article(df):
    print("Number of revisions per article:")
    df_edits = df.groupBy("title").agg(f.count("*").alias("edit history length"))\
        .orderBy(desc("edit history length"))
    return df_edits


def draw_histogram(df1, df2):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    axes[0, 1].set_ylim([0, 200])
    hist(axes[0, 0], [df1], bins=20, color=['red'])
    axes[0, 0].set_title('Anzahl von Revisionen ueber allen Artikeln')
    axes[0, 0].set_xlabel('Anzahl der Revisionen')
    axes[0, 0].set_ylabel('Anzahl der Artikeln')
    hist(axes[0, 1], [df2], bins=20, color=['blue'])
    axes[0, 1].set_title('Anzahl von Revisionen ueber Autoren pro Artikel')
    axes[0, 1].set_xlabel('Anzahl der Revisionen per Autor')
    axes[0, 1].set_ylabel('Anzahl der Artikeln')
    plt.savefig('Number_of_revisions_per_article')

	
#get start time
start_time = time.time()

df = load_to_spark.main_init_df()

#retrieve spark worker count
worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1

total_edits_per_article = number_of_revisions_per_article(df)
total_edits_per_article.cache()
total_edits_per_article.show()
df_revision_length = total_edits_per_article\
    .select(col("edit history length").alias("revision_length"))\
    .orderBy(desc("revision_length"))


total_number_of_authors_per_article = number_of_authors_per_article(df)
total_number_of_authors_per_article.cache()
total_number_of_authors_per_article.show()

df_all_authors = total_number_of_authors_per_article.where(col("author").isNotNull()).distinct()

df_bots = df_all_authors.where(col("author").rlike('|'.join(search_text)))

df_real_users = df_all_authors.subtract(df_bots)

df_number_of_edits_of_author = df_real_users\
    .select(col("count").alias("number of edits of author per article"))\
    .orderBy(asc("number of edits of author per article"))

df_number_of_edits_of_bots = df_bots\
    .select(col("count").alias("number of edits of bots per article"))\
    .orderBy(asc("number of edits of bots per article"))

df_number_of_edits_of_author.cache()
print(df_number_of_edits_of_author.count())

#get end time
end_time = time.time()

draw_histogram(df_revision_length, df_number_of_edits_of_author)

#calculate duration and write the application information to the log file
duration = end_time - start_time
file = open(logpath, 'a+')
output = '{} {} {}\n'.format(worker_count, file_count, duration)
file.write(output)

print('DONE')
