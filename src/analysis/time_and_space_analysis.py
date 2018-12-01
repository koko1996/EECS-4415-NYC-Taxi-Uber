from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd

#  create spark configuration
conf = SparkConf()
conf.setAppName("TimeSpaceAnalysis")
# create spark context with the above configuration
sc = SparkContext(conf=conf)

def lineMapper(line):
    words = line.split(",")
    pickUpTime = words[1]
    pickUpHour = pickUpTime.split(":")[0].strip()
    pickUpHour = int( int(pickUpHour) / 4)
    return ((words[0],str(pickUpHour),words[2]),1)


# data = sc.textFile("/taxi_sample_clean.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2]).saveAsTextFile("/taxi_processed")
taxiData = sc.textFile("/taxi_sample_clean.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2])
print ( taxiData.collect())

uberData = sc.textFile("/uber_clean_sample.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2])


print ( uberData.collect())
