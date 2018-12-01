from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd

#  create spark configuration
conf = SparkConf()
conf.setAppName("BusinessImprovement")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

taxiFile = sc.textFile("/taxi_sample_clean.csv")
taxiFile.cache()
taxiCount = taxiFile.map(lambda x: ("Taxi",1) ).reduceByKey(lambda x,y: x + y).collect()
print ( taxiCount)

sc.broadcast(taxiCount)

def taxiLineMapper(line):
    words = line.split(",")
    pickUpTime = words[0]
    pickUpYear = pickUpTime.split("-")[0]
    count = taxiCount[0][1]
    return ((pickUpYear,words[2]),1.0/count)

# data = sc.textFile("/taxi_sample_clean.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2]).saveAsTextFile("/taxi_processed")
taxiData = taxiFile.map(taxiLineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][1])
print ( taxiData.collect())

taxiFile.unpersist()

uberData = sc.textFile("/uber_clean_sample.csv")
uberData.cache()
uberCount = uberData.map(lambda x: ("Uber",1) ).reduceByKey(lambda x,y: x + y).collect()
print ( uberCount)

sc.broadcast(uberCount)

def uberLineMapper(line):
    words = line.split(",")
    pickUpTime = words[0]
    pickUpYear = pickUpTime.split("-")[0]
    count = uberCount[0][1]
    return ((pickUpYear,words[2]),1.0/count)

uberData = uberData.map(uberLineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0])
print ( uberData.collect())
uberData.unpersist()