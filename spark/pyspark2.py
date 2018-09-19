# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 05:57:36 2018

@author: fischpet
"""

from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f
from pyspark.sql.functions import rank, min, avg, explode, col, desc, min as min_, max as max_
import load_to_spark

import random

randvalues = random.sample(range(1, 100), 50)

alltslist = [(x,) for x in range(15)]

allgroups = [(g,) for g in range(2)]

step = 31*60*60*24


def create_rand_pairs(length=50):
    id_rand_pairs = []
    for i in range(length):
        id_rand_pairs.append((i, randvalues[i]))
    return id_rand_pairs


def create_rand_pairs_df(length=50):
    return spark.createDataFrame(create_rand_pairs(length), ['id', 'value'])


def create_rand_pairs_groups_df(num_groups=2, length=50):
    group_traces = []
    for i in range(num_groups):
        randPairs = create_rand_pairs(length)
        for rP in randPairs:
            group_traces.append((i, rP[0], rP[1]))
    random.shuffle(group_traces)
    return spark.createDataFrame(group_traces, ['group', 'ts', 'val'])


def create_rand_pairs_groups_nested_df(num_groups=2, length=50):
    group_traces = []
    for i in range(num_groups):
        randPairs = create_rand_pairs(length)
        group_traces.append((i, randPairs))
    random.shuffle(group_traces)
    return spark.createDataFrame(group_traces, ['group', 'revs'])


spark = SparkSession \
        .builder \
        .appName("whatever") \
        .getOrCreate()


df_gn = create_rand_pairs_groups_nested_df(2, 10)

df_ts = spark.createDataFrame(alltslist, ["ts_ref"])

df_groups = spark.createDataFrame(allgroups, ['group_ref'])

df_group_ts = df_groups.crossJoin(df_ts)
print("df_groups_ts")
df_group_ts.show()

df_p1 = df_gn.select('group', explode('revs'))
df_p2 = df_p1.select('group', col('col._1').alias('ts'), col('col._2').alias('val'))
print('df_p2')
df_p2.show(50)

df_allts = df_group_ts.join(df_p2, (df_group_ts.group_ref == df_p2.group) & (df_group_ts.ts_ref == df_p2.ts), how='left')\
    .orderBy(col('group_ref'), col('ts_ref')).select(col('group_ref'), col('ts_ref'), col('val'))

df_allts.show(100)


#print (df_p.agg({"id": "avg","value":"max"}).collect())

window = Window.partitionBy("group_ref").orderBy('ts_ref').rowsBetween(-1, 1)
df_allts.select('group_ref', 'ts_ref', 'val', avg('val').over(window)).na.fill(0).show(40)



#conf = SparkConf().setAppName('bla').setMaster('local')


#df_range = spark.range(1,100,5)

#df_range.show()

#rdX = df_range.select('id').foreach(lambda l: l[0]+2)

##print (df_range.agg({"*": "avg"}).collect())


#print(rdX.collect())

#print (df_range.schema)
