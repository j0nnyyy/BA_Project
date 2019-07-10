from pyspark.sql.functions import col
from pyspark.sql import Row
import load_to_spark
import jaccard_similarity
import argparse
import matplotlib.pyplot as plt
from pyspark_dist_explore import hist

plot_path = "/scratch/wikipedia-dump/plots/jaccard/article_category_distance.png"
base_path = "/scratch/wikipedia-dump/wiki_small_"
category_file = "/scratch/wikipedia-dump/categorylinks.json"
bots = ["Bot", "Bots"]
filenames = []

def draw_histograms(df_sim, df_rest, df_diff):
    plt.rcParams.update({'font.size': 28})
    fig, axes = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)
    hist(axes[0, 0], [df_sim], bins=20, color=['red'])
    axes[0, 0].set_title("Ähnlichkeit der Kategorien ähnlicher Artikel")
    axes[0, 0].set_xlabel("Jaccard Distanz")
    axes[0, 0].set_ylabel("Anzahl der Artikelpaare")
    hist(axes[0, 1], [df_rest], bins=20, color=['red'])
    axes[0, 1].set_title("Ähnlichkeit der Kategorien der restlichen Artikel")
    axes[0, 1].set_xlabel("Jaccard Distanz")
    axes[0, 1].set_ylabel("Anzahl der Artikelpaare")
    hist(axes[1, 0], [df_diff], bins=20, color=['red'])
    axes[1, 0].set_title("Ähnlichkeit der Kategorien verschiedener Artikel")
    axes[1, 0].set_xlabel("Jaccard Distanz")
    axes[1, 0].set_ylabel("Anzahl der Artikelpaare")
    plt.savefig(plot_path)

def join_by_columns(df1, df2, col1, col2):
    dfa = df1.select("*", col(col1).alias(col1 + "1"), col(col2).alias(col2 + "2"))\
        .drop(col1, col2)
    dfb = df2
    df_joined = dfa.join(dfb, (col(col1) == col(col1 + "1")) & (col(col2) == col(col2 + "2")))\
        .drop(col1 + "1", col2 + "2")
    return df_joined

parser = argparse.ArgumentParser()
parser.add_argument("--filenumber")
parser.add_argument("--filecount")
args = parser.parse_args()

if args.filecount:
    filecount = int(args.filecount)
    if filecount == 1:
        if args.filenumber:
            filenumber = args.filenumber
            filenames.append(base_path + filenumber + ".json")
        else:
            filenames.append(base_path + "1.json")
    else:
        for i in range(1, filecount + 1):
            filenames.append(base_path + str(i) + ".json")
elif args.filenumber:
    filenames.append(base_path + args.filenumber + ".json")
else:
    filenames.append(base_path + "1.json")

df_categories = load_to_spark.create_category_df(category_file)
df = load_to_spark.main_init_df(filenames)
#df = load_to_spark.get_samples(filenames, 100000)
df.cache()
'''
load_to_spark.create_session()
df = load_to_spark.sc.parallelize([
    Row(author="A", title="X", id="1"),
    Row(author="B", title="X", id="1"),
    Row(author="C", title="X", id="1"),
    Row(author="A", title="Y", id="2"),
    Row(author="B", title="Y", id="2"),
    Row(author="B", title="Y", id="2"),
    Row(author="C", title="Y", id="2"),
    Row(author="A", title="Z", id="3"),
    Row(author="A", title="Z", id="3"),
    Row(author="B", title="Z", id="3"),
    Row(author="D", title="U", id="4")
]).toDF()

df_categories = load_to_spark.sc.parallelize([
    Row(id="1", category="P"),
    Row(id="1", category="Q"),
    Row(id="1", category="R"),
    Row(id="1", category="S"),
    Row(id="2", category="P"),
    Row(id="2", category="Q"),
    Row(id="2", category="R"),
    Row(id="3", category="P"),
    Row(id="3", category="Q"),
    Row(id="4", category="T")
]).toDF()
'''

#remove bots
df = df.where(col("author").isNotNull())
df_bots = df.where(col("author").rlike("|".join(bots)))
df = df.subtract(df_bots)
df.show()

df_categories = df_categories.select(col("category"), col("id").alias("id1")).distinct()

#title|author|category
df_joined = df.join(df_categories, col("id") == col("id1")).select("title", "author", "category")

#title|category
df_t_c = df_joined.select("title", "category")
df_t_a = df_joined.select("title", "author")

#title|author|jaccard
df_jaccard = jaccard_similarity.jaccard_with_min_hashing(df_t_a, "title", "author", maxval=0.9)
df_jaccard.cache()
df_jaccard.show()

#title1|title2
df_similar = df_jaccard.where(col("jaccard") < 0.3).select("title1", "title2")
df_similar.show()
df_rest = df_jaccard.where((col("jaccard") >= 0.3) & (col("jaccard") <= 0.7)).select("title1", "title2")
df_rest.show()

#title|category
df_c_sim = df_t_c.join(df_similar, col("title") == col("title1")).select("title", "category").distinct()
df_c_sim = df_c_sim.union(df_t_c.join(df_similar, col("title") == col("title2")).select("title", "category").distinct())
df_c_sim.show()
df_c_rest = df_t_c.join(df_rest, col("title") == col("title1")).select("title", "category").distinct()
df_c_rest = df_c_rest.union(df_t_c.join(df_rest, col("title") == col("title2")).select("title", "category").distinct())
df_c_rest.show()
df_c_diff = df_t_c.subtract(df_c_sim).subtract(df_c_rest).distinct()
df_c_diff.show()

#title|category|jaccard
print("calculating jaccard")
df_jacc_sim = jaccard_similarity.jaccard_with_min_hashing(df_c_sim, "title", "category")
df_jacc_sim.show()
df_jacc_rest = jaccard_similarity.jaccard_with_min_hashing(df_c_rest, "title", "category")
df_jacc_rest.show()
df_jacc_diff = jaccard_similarity.jaccard_with_min_hashing(df_c_diff, "title", "category")
df_jacc_diff.show()

print("selecting desired values")
df_sim = join_by_columns(df_jacc_sim, df_similar, "title1", "title2").select("title1", "title2", "jaccard")
df_sim.show()
df_rest = join_by_columns(df_jacc_rest, df_rest, "title1", "title2").select("title1", "title2", "jaccard")
df_rest.show()
df_diff = df_jacc_diff.subtract(df_sim).subtract(df_rest)
df_diff.show()

#jaccard
df_hist_sim = df_sim.select("jaccard")
df_hist_rest = df_rest.select("jaccard")
df_hist_diff = df_diff.select("jaccard")

print("Drawing histograms")
draw_histograms(df_hist_sim, df_hist_rest, df_hist_diff)
