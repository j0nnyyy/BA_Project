import load_to_spark
import time

load_to_spark.create_session()
print(load_to_spark.sc.defaultParallelism)

def test():
    start = time.time()
    df = load_to_spark.main_init_df("/scratch/wikipedia-dump/wiki_small_1.json")
    print("test")
    df.count()
    end = time.time()
    print(end - start)

test()
