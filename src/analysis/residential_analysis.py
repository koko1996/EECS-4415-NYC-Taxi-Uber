from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from dateutil.parser import parse
from operator import add
import pyspark
import sys
import requests


conf = SparkConf()
conf.setAppName("Residential_Analysis")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)
# read the csv file
uberFile = sc.textFile("/uber_combined.csv")
taxiFile = sc.textFile("/taxi_combined.csv")


#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.

# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#((pickup_date,borough), value) where the pickup date only contain the year and the month
def mapper(x,value):
	# parse the date
	date = parse(x[0])
	# extract the year and the month and map them to 2
	return((str(date.year) + "-" + str(date.month),x[2]),value)

# for each line in the Uber file, this line will map the pickup_date to tuple, check the mapper function for details about the tuple.
uber_temp = uberFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))
taxi_temp = taxiFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,1))

# reduce by key to get the total number of rides in NYC in a month
uber_temp= uber_temp.reduceByKey(add).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][1])
taxi_temp= taxi_temp.reduceByKey(add).sortBy(lambda a: a[0][0]).sortBy(lambda a: a[0][1])

# remove this line before submitting the code.
print("Number of uber rides per month per zone")
print(uber_temp.collect())
uber_temp.saveAsTextFile("/uber_residential_processed")


print("Number of taxi rides per month per zone")
print(taxi_temp.collect())
taxi_temp.saveAsTextFile("/taxi_residential_processed")







 
