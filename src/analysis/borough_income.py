from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd

#  create spark configuration
conf = SparkConf()
conf.setAppName("Borough_Income")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# mapper that takes triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#(borough, 1) where the borough contains the one of the borough names in New York City
def lineMapper(line):
    words = line.split(",")
    return (words[2],1)

#-----------Taxi Analysis

# Read the Taxi data from HDFS, Map it to (borough, 1) tuples, reduce it by key (borough) and the sort it by borough for better formatted output
taxiData = sc.textFile("/taxi_combined.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0])
print ( taxiData.collect())
taxiData.saveAsTextFile("/taxi_income_processed")


#-----------Uber Analysis


# Read the Uber data from HDFS, Map it to (borough, 1) tuples, reduce it by key (borough) and the sort it by borough for better formatted output
uberData = sc.textFile("/uber_combined.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0])
print ( uberData.collect())
uberData.saveAsTextFile("/uber_income_processed")
