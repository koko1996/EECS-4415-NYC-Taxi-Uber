from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
import pandas as pd

conf = SparkConf()
conf.setAppName("PreProcessingApp")
sc = SparkContext(conf=conf)
sql_sc = SQLContext(sc)

pandas_df = pd.read_csv('temp.csv') 
df = sql_sc.createDataFrame(pandas_df)

print(df.collect())
