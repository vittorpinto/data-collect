#%%
import pandas as pd
import os
import json
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master ("local[*]") \
    .config("spark.some.config.option", "some-value") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:///path/to/log4j.properties") \
    .appName("pokemon-bronze") \
    .getOrCreate()

#%%
# os.chdir("./pokemon")
os.getcwd()
# %%
file = "/mnt/c/Users/xerel/OneDrive/Área de Trabalho/Github/data-collect/pokemon/data/pokemon/"

df = spark.read.json(file)
# df.printSchema()
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
df_explode_pokemon.show(20, truncate=False)

#%% Criando uma window e escolhendo o ultimo pokemon que foi ingerido
window_spec = Window.partitionBy('name').orderBy(col('ingestion_date').desc())

df_with_row_num = df_explode_pokemon.withColumn('row_num',row_number().over(window_spec))

df_final = df_with_row_num.filter(col('row_num') == 1).drop('row_num')


#%%
df_final.show(10, truncate=False)
# %%
spark.sql(
    'SELECT * FROM Pokemons \
    QUALIFY ROW_NUMBER() OVER (PARTITION BY Pokemons.name ORDER BY ingestion_date desc) = 1'
).show()

# %%
