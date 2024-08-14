#%%
import pandas as pd
import os
import json

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

os.chdir("../pokemon")
# %%
file = "./data/pokemon/2024-08-13_21-50-17-799282.json"

df = spark.read.json(file)
# df.printSchema()
df.createOrReplaceTempView("pokemon")
# %%
