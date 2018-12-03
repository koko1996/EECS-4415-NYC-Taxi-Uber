from pyspark import SparkConf,SparkContext

from pyspark.sql import SQLContext

from pyspark.sql.types import *

from operator import add

import pyspark

import sys

import requests

import datetime





#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.



# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format

#(weekyday in a numer value, value) 

def mapper(x,value):
	# pares the date
	year, month, day = (int(k) for k in x[0].split('-'))
	# extract the weekday
	# weekday will return a number between 0 and 6, where 0 stands for Monday and 6 for Sunday
	# This is the main reason behind Weekdays
	answer = datetime.date(year, month, day).weekday()
	# return a tuple
	return((answer,x[0]),value)


# This used to determine day of week
Weekdays=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

conf = SparkConf()
conf.setAppName("Taxi_vs_Uber")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)

# read the csv file

#----------- Uber
uberFile = sc.textFile("/uber_combined.csv")

# for each line in the Uber file, this line will map the pickup_date to tuple, check the mapper function for details about the tuple.
uber_temp = uberFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))

# reduce by key to get the total number of rides per weekday
uber_temp= uber_temp.reduceByKey(add)

# map each entry so you get the number of rides per day 
uber_temp= uber_temp.map(lambda x : (x[0][0],x[1]))

# count the Number of weekdays, this is used to calculate the average
NumberOfWeekdaysUber = uber_temp.countByKey()

# brodcast
sc.broadcast(NumberOfWeekdaysUber)


# reduce by key to get the total number of rides per weekday
uber_temp= uber_temp.reduceByKey(add)



# convert weekday numeric value to English and calculate the average
uber_temp = uber_temp.map(lambda x: (Weekdays[x[0]],x[1]/NumberOfWeekdaysUber[x[0]]))

print("Average Number of Uber rides per weekday")
print(uber_temp.collect())

#---- taxi
taxiFile = sc.textFile("/taxi_combined.csv")

taxi_temp = taxiFile.map(lambda line: line.split(",")).map(lambda x : mapper(x,1))
# map each entry so you get the number of rides per day 
taxi_temp= taxi_temp.map(lambda x : (x[0][0],x[1]))

# reduce by key to get the total number of rides per weekday
taxi_temp= taxi_temp.reduceByKey(add)

# count the Number of weekdays, this is used to calculate the average
NumberOfWeekdaysTaxi = taxi_temp.countByKey()

# brodcast
sc.broadcast(NumberOfWeekdaysTaxi)

# reduce by key to get the total number of rides per weekday
taxi_temp= taxi_temp.reduceByKey(add)

# convert weekday numeric value to English and calculate the average
taxi_temp = taxi_temp.map(lambda x: (Weekdays[x[0]],x[1]/NumberOfWeekdaysTaxi[x[0]]))

print("Average Number of taxi rides per weekday")
print(taxi_temp.collect())