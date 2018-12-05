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
from dateutil.parser import parse
from datetime import datetime


"""
RIDE PER MONTH
spark-submit sparkplot-business-improvement
"""

# takes a line in this format [pickup_date,pickup_time,borough] and returns a tuple in the following format
#(pickupYear-pickupMonth,borough) where borough is the integer representation of the borough 
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
    date = parse(x[0])
    return ((str(date.year) + "-" + str(date.month),value),1)

def date_boro_aggr(x):
    """
    convert the [ [(year-month,borough),count],[(year-month,borough),count],[(year-month,borough),count],[(year-month,borough),count],[(year-month,borough),count]] where year-month is the same for all the entries of this array since we do group be before this mapping and boroughs are distinct
    to [list of aggregated borough counts] (we will have six versions of this each representing apr may jun of 2014 and 2015)
    """
    temp = [0,0,0,0,0]
    for ele in x[1]:
        temp[int(ele[0][1])] += int(ele[1])
    return temp

n_of_periods = 3 #3 month for now


conf = SparkConf()
conf.setAppName("Residential_Analysis")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

#------Taxi Analysis

# read the csv file  which has no header
taxiFileWithNoHeader = sc.textFile("/taxi_combined.csv")

# map each entry to ((year-month, borough integer representaion),1) 
taxi_date_boro = taxiFileWithNoHeader.map(lambda line: line.split(",")).map(date_boro_mapper)

# reduce each mapped tuple by borough and year-month, then group by year-month so that we will have combined
# data for only the months
taxi_d_b_group = taxi_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])

# sort the tuples by time frame (there should be 6 elements here hence sorting is not an issue)
taxi_d_b_sorted_group = taxi_d_b_group.sortBy(lambda x: x[0][0])

# map the tuples to an array format to make the conversion to dataframe easy
taxi_d_b_sorted_group_d_f_format = taxi_d_b_sorted_group.map(date_boro_aggr)

# convert out RDD to dataframe for the plotting
taxi_visu_data_frame = pd.DataFrame(taxi_d_b_sorted_group_d_f_format.collect(),index = [datetime.strptime(item[0], "%Y-%m") for item in taxi_d_b_sorted_group.collect()])

# create the data frame for the given ranges with given frequencies
taxi_datetime_index = pd.date_range('2014-04-01', periods= n_of_periods, freq='MS')
taxi_visu = pd.DataFrame(index=taxi_datetime_index)

# add the values of the RDD to the dataframe and fill zero in empty space 
taxi_visu = taxi_visu.add(taxi_visu_data_frame, fill_value = 0).fillna(0)
# print the output to stdout to make sure everything is as expected
pprint(taxi_visu)
# create the html file for visualization 
graph(taxi_visu,len(taxi_visu),'M','Monthly count per borough for taxi','2014-04')


#------Uber Analysis

# read the csv file which has no header
uberFileWithNoHeader = sc.textFile("/uber_combined.csv")

# map each entry to ((year-month, borough integer representaion),1) 
uber_date_boro = uberFileWithNoHeader.map(lambda line: line.split(",")).map(date_boro_mapper)

# reduce each mapped tuple by borough and year-month, then group by year-month so that we will have combined
# data for only the months
uber_d_b_group = uber_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])

# sort the tuples by time frame (there should be 6 elements here hence sorting is not an issue)
uber_d_b_sorted_group = uber_d_b_group.sortBy(lambda x: x[0][0])

# map the tuples to an array format to make the conversion to dataframe easy
uber_d_b_sorted_group_d_f_format = uber_d_b_sorted_group.map(date_boro_aggr)

# convert out RDD to dataframe for the plotting
uber_visu_data_frame = pd.DataFrame(uber_d_b_sorted_group_d_f_format.collect(),index = [datetime.strptime(item[0], "%Y-%m") for item in uber_d_b_sorted_group.collect()])

# create the data frame for the given ranges with given frequencies
uber_datetime_index = pd.date_range('2014-04-01', periods= n_of_periods, freq='MS')
uber_visu = pd.DataFrame(index=uber_datetime_index)
# add the values of the RDD to the dataframe and fill zero in empty space 
uber_visu = uber_visu.add(uber_visu_data_frame, fill_value = 0).fillna(0)
# print the output to stdout to make sure everything is as expected
pprint(uber_visu)

# create the html file for visualization 
graph(uber_visu,len(uber_visu),'M','Monthly count per borough for uber','2014-04')
