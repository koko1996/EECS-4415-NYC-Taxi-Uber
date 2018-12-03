from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import pandas as pd

#  create spark configuration
conf = SparkConf()
conf.setAppName("Business_Improvement")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# mapper that takes triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#(Pickup Year, 1) where the Pickup Year is the year in the pickup_date
def yearMapper(line):
    words = line.split(",")
    pickUpTime = words[0]
    pickUpYear = pickUpTime.split("-")[0]
    return (pickUpYear,1)


#-----------Taxi Analysis

# read the Taxi file (no headers in the file), the file should be on HDFS
taxiFile = sc.textFile("/taxi_combined.csv")

# cache the data because it is going to be used multiple times
taxiFile.cache()

# Total count of the Taxi data by year (the rdd will contain two elements at most guven we are only analysing for 2014 and 2015 only)
taxiCount = taxiFile.map(yearMapper).reduceByKey(lambda x,y: x + y).collect()
print ( taxiCount)

# Brodcast the count of taxis to all the worker nodes
sc.broadcast(taxiCount)

# mapper that takes triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#((Pickup Year, borough), percentage) where the Pickup Year is the year of pickup_date and borough is one of the borough names in New York City and percentage represents one divided by Total number of Taxi rides in that year in NYC
def taxiLineMapper(line):
    words = line.split(",")
    pickUpTime = words[0]
    pickUpYear = pickUpTime.split("-")[0]
    count = 1
    for year, total in taxiCount:
        if year == pickUpYear:
            count = total
    return ((pickUpYear,words[2]),1.0/count)


# Calculate the percentage of Taxi Rides in each borough for each year divided by the total number of Taxi Rides in that year in all the boroughs of New York City
taxiData = taxiFile.map(taxiLineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][1])
print ( taxiData.collect())
taxiData.saveAsTextFile("/taxi_bussiness_processed")

# Drop the taxi data from memory to speed up the other calculations
taxiFile.unpersist()

#-----------Uber Analysis

# read the Uber file (no headers in the file), the file should be on HDFS
uberData = sc.textFile("/uber_combined.csv")
# Cache the data because it is going to be used multiple times
uberData.cache()

# Total count of the Uber data by year (the rdd will contain two elements at most guven we are only analysing for 2014 and 2015 only)
uberCount = uberData.map(yearMapper).reduceByKey(lambda x,y: x + y).collect()
print ( uberCount)

# Brodcast the count of Uber to all the worker nodes
sc.broadcast(uberCount)

# mapper that takes triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#((Pickup Year, borough), percentage) where the Pickup Year is the year of pickup_date and borough is one of the borough names in New York City and percentage represents one divided by Total number of Uber rides in that year in NYC
def uberLineMapper(line):
    words = line.split(",")
    pickUpTime = words[0]
    pickUpYear = pickUpTime.split("-")[0]
    count = 1
    for year, total in uberCount:
        if year == pickUpYear:
            count = total
    return ((pickUpYear,words[2]),1.0/count)    


# Calculate the percentage of Uber Rides in each borough for each year divided by the total number of Uber Rides in that year in all the boroughs of New York City
uberData = uberData.map(uberLineMapper).reduceByKey(lambda x,y: x + y).sortBy(lambda a: a[0]).sortBy(lambda a: a[0][1])
print ( uberData.collect())
uberData.saveAsTextFile("/uber_bussiness_processed")

# Droping the Uber data from memory is not necessary at this point since we are done