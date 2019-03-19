#changed matplotlib display from its default value to enable plot saving
import matplotlib
matplotlib.use('Agg')

from pyspark.sql.functions import desc, col, asc
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist
import load_to_spark

search_text = ['Bot', 'Bots']
logpath = '/home/ubuntu/BA_Project/log.txt'

#retrieve loaded file count
file_count = load_to_spark.filename.count(',') + 1

# total number of edits per author
def numbder_of_revisions_per_author(df):
    print("Number of edits per author")
    authors = df.groupBy("author").count().orderBy(desc("count"))
    return authors


def revisions_per_author():
    df = load_to_spark.main_init_df()
    return numbder_of_revisions_per_author(df)


def draw_histogram(df1, df2):
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    axes[0, 0].set_ylim([0, 1000])
    hist(axes[0, 0], [df1, df2], bins=10, color=['red', 'tan'])
    axes[0, 0].legend()
    axes[0, 0].set_title('Anzahl Revisionen zwischen Bots und Benutzer')
    axes[0, 0].set_xlabel('Laenge der Revisionen')
    axes[0, 0].set_ylabel('Anzahl Autoren/Bots')
    plt.savefig('Number_of_revisions_per_author(bots,real_users)')

#get start time
start_time = time.time()

print("Number of revisions for all authors:")
df = revisions_per_author()

#retrieve spark worker count
worker_count = load_to_spark.sc._jsc.sc().getExecutorMemoryStatus().size() - 1

df_all_authors = df.where(col("author").isNotNull()).distinct()
print('All authors count = ', df_all_authors.count())
df_all_authors.cache()
df_all_authors.show()

df_percent = df_all_authors.withColumn('percent', f.round(col('count')/f.sum('count').over(Window.partitionBy()), 2))\
    .orderBy(desc('percent'))
df_percent.cache()
df_percent.show()

print("Select only Bots:")
df_bots = df_all_authors.where(col("author").rlike('|'.join(search_text)))
df_bots.cache()
df_bots.show()
print('Bots count = ', df_bots.count())


print("Select all authors except bots:")
df_real_users = df_all_authors.subtract(df_bots)
df_real_users.cache()
df_real_users.show()
print('Real users count = ', df_real_users.count())

df_bots_hist = df_bots.select(col("count").alias("bots"))
df_users_hist = df_real_users.select(col("count").alias("real users"))

#get end time
end_time = time.time()

draw_histogram(df_bots_hist, df_users_hist)

#calculate duration and write the application information to the log file
duration = end_time - start_time
file = open(logpath, 'a+')
file.write(worker_count, file_count, duration)

print('DONE')
