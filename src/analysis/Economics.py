from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from dateutil.parser import parse
from operator import add
import pyspark
import sys
import requests


conf = SparkConf()
conf.setAppName("Economics")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)

#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.

# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#(pickup_date, value) where the pickup date only contain the year and the month
def mapper(x,value):
	# parse the date
	date = parse(x[0])
	# extract the year and the month and map them to 2
	return(str(date.year) + "-" + str(date.month),value)

# read the csv file
uberFile = sc.textFile("/uber_combined.csv")

# for each line in the Uber file, this line will map the pickup_date to 2, where the pickup date only contains the month and the year.
uber_temp = uberFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))

# reduce by key to get the total number of rides in NYC in a month
uber_temp= uber_temp.reduceByKey(add).sortBy(lambda a: a[0])

# remove this line before submitting the code.
print("Number of uber rides per month")
print(uber_temp.collect())
uber_temp.saveAsTextFile("/uber_economics_processed")


# read the csv file
taxiFile = sc.textFile("/taxi_combined.csv")

# for each line in the Uber file, this line will map the pickup_date to 2, where the pickup date only contains the month and the year.
taxi_temp = taxiFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,1))

# reduce by key to get the total number of rides in NYC in a month
taxi_temp= taxi_temp.reduceByKey(add).sortBy(lambda a: a[0])

print("Number of taxi rides per month")
print(taxi_temp.collect())
taxi_temp.saveAsTextFile("/taxi_economics_processed")








 
