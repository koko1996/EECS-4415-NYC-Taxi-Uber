from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from operator import add
import pyspark
import sys
import requests
from pprint import pprint
import pandas as pd
import numpy as np
from timescaleplot import graph
conf = SparkConf()
conf.setAppName("Taxi_vs_Uber")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)
# read the csv file
#uberFile = sc.textFile("file:///app/plot/uber_clean_sample.csv")
taxiFile = sc.textFile("file:///app/plot/taxi_clean_sample.csv")

# remove the header from UberFile
#uberHeader = uberFile.filter(lambda line: "Pickup_time" in line)
#uberFileWithNoHeader = uberFile.subtract(uberHeader)

#------------I know that his code is somehow contains duplicate code, but for readablity purposes I will keep it as it is

# remove the header from Taxi File
taxiHeader = taxiFile.filter(lambda line: "Pickup_time" in line)
taxiFileWithNoHeader = taxiFile.subtract(taxiHeader)


#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.

# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#(pickup_date, value) where the pickup date only contain the year and the month
def date_boro_mapper(x):
    """
    simply map each borough to integer representation
    borough title :
    0 is Staten Island
    1 is Queens
    2 is Brooklyn
    3 is manhatan
    4 is Bronx
    """
    if x[2] == 'Staten Island':
        value = 0
    elif x[2] == 'Queens':
        value = 1
    elif x[2] == 'Brooklyn':
        value = 2
    elif x[2] == 'Manhattan':
        value = 3
    else:
        value = 4
    return(x[0],value)
def date_boro_aggr(x):
    """
    convert the [date,list of all ride in integer borough code]
    to [date,list of aggregated boroughs]
    """
    temp = [0,0,0,0,0]
    for ele in x[1]:
        temp[int(ele[1])] += 1
    return temp

# for each line in the Uber file, this line will map the pickup_date to 2, where the pickup date only contains the month and the year.
#uber_temp = uberFileWithNoHeader.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))

# reduce by key to get the total number of rides in NYC in a month
#uber_temp= uber_temp.reduceByKey(add)
taxi_date_boro = taxiFileWithNoHeader.map(lambda line: line.split(",")).map(date_boro_mapper)
taxi_d_b_group = taxi_date_boro.groupBy(lambda x: x[0])
taxi_d_b_sorted_group = taxi_d_b_group.sortBy(lambda x: x[0])
taxi_d_b_sorted_group_d_f_format = taxi_d_b_sorted_group.map(date_boro_aggr)
#dataframe that will be used for plotting
visu_data_frame = pd.DataFrame(taxi_d_b_sorted_group_d_f_format.collect())
pprint(visu_data_frame)
pprint(len(visu_data_frame))
graph(visu_data_frame,len(visu_data_frame),'M','Test','2015-04-01')
