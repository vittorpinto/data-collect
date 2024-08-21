#%%
import pandas as pd
import os
import json
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col

spark = SparkSession.builder \
    .master ("local") \
    .config("spark.some.config.option", "some-value") \
    .appName("pokemon-bronze") \
    .getOrCreate()

#%%
# os.chdir("./pokemon")
os.getcwd()
# %%
file = "/mnt/c/Users/xerel/OneDrive/Área de Trabalho/Github/data-collect/pokemon/data/pokemon/2024-08-13_16-42-40-782213.json"

df = spark.read.json(file)
# df.printSchema()
df.createOrReplaceTempView("pokemon")
#%%
df.printSchema
df.show()
#%%
df_explode = df.select(
    'ingestion_date',explode(df.results).alias('pokemons')
    )

df_explode_pokemon = df_explode.select(
    col('ingestion_date'),
    col('pokemons.name').alias('name'),
    col('pokemons.url').alias('link')
    )

#%%
df_explode.printSchema()
#%%
df_explode_pokemon.show(10, truncate=False)

# %%
spark.sql(
    'SELECT * FROM pokemon '
).show()

# %%
