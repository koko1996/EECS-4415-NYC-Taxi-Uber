from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd

#  create spark configuration
conf = SparkConf()
conf.setAppName("Time_Space_Analysis")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# mapper that takes triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#((Pickup date, Pickup Hour,borough), 1) where the Pickup Date is the same as pickup_date and  Pickup Hour is the hour of the pickup it takes on the values {0,1,2,3} where 0 represent hours from  00:00:00 to 06:00:00 and 1 represents hours from 06:00:00 to 12:00:00 and so on and borough is one of the borough names in New York City
def lineMapper(line):
    words = line.split(",")
    pickUpTime = words[1]
    pickUpHour = pickUpTime.split(":")[0].strip()
    pickUpHour = int( int(pickUpHour) / 4)
    return ((words[0],str(pickUpHour),words[2]),1)


#-----------Taxi Analysis

# Read the Taxi data from HDFS, Map it to ((Pickup date, Pickup Hour,borough), 1) tuples, reduce it by key (Pickup date, Pickup Hour,borough) and the sort it for better formatted output
taxiData = sc.textFile("/taxi_combined.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2])
print ( taxiData.collect())
taxiData.saveAsTextFile("/taxi_time_space_processed")

#-----------Uber Analysis

# Read the Uber data from HDFS, Map it to ((Pickup date, Pickup Hour,borough), 1) tuples, reduce it by key (Pickup date, Pickup Hour,borough) and the sort it for better formatted output
uberData = sc.textFile("/uber_combined.csv").map(lineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][1]).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][2])
print ( uberData.collect())
uberData.saveAsTextFile("/uber_time_space_processed")
