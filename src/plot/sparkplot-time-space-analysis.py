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
from datetime import datetime


# mapper that maps each entry to ((timeFrame, borough integer representaion),1)
def date_boro_mapper(x):
    """
    map each borough to integer representation
    borough title :
    0 is Staten Island
    1 is Queens
    2 is Brooklyn
    3 is manhatan
    4 is Bronx
    """
    if x[2] == 'Staten Island':
        borough = 0
    elif x[2] == 'Queens':
        borough = 1
    elif x[2] == 'Brooklyn':
        borough = 2
    elif x[2] == 'Manhattan':
        borough = 3
    else:
        borough = 4

    """
    simply map each time  to its time frame
    borough title :
    00-04 is 00
    04-08 is 04
    08-12 is 08
    12-16 is 12
    16-20 is 16
    20-24 is 20
    """
    timeFrame = x[1]
    if(x[1] <= "23:59:59"):
        timeFrame = '20:00:00'
    if(x[1] <= "19:59:59"):
        timeFrame = '16:00:00'
    if(x[1] <= "15:59:59"):
        timeFrame = '12:00:00'
    if(x[1] <= "011:59:59"):
        timeFrame = '08:00:00'
    if(x[1] <= "07:59:59"):
        timeFrame = '04:00:00'
    if(x[1] <= "03:59:59"):
        timeFrame = '00:00:00'

    return ((timeFrame,borough),1)

def date_boro_aggr(x):
    """
    convert the [ [(time,borough),count],[(time,borough),count],[(time,borough),count],[(time,borough),count],[(time,borough),count]] where time is the same for all the entries of this array and boroughs are distinct
    to [list of aggregated borough counts] (we will have six versions of this each representing one time frame)
    """
    temp = [0,0,0,0,0]
    for ele in x[1]:
        temp[int(ele[0][1])] += int(ele[1])
    return temp

# configuration for the reuslts that are going to be generated
n_of_periods = 6
start_date = '2014'
start_time = ''
freq = '4H'

# spark config
conf = SparkConf()
conf.setAppName("html_plot_time_space_analysis")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)

#------Taxi Analysis

# read the csv file
taxiFile = sc.textFile("/taxi_combined.csv")

# map each entry to ((timeFrame, borough integer representaion),1)
taxi_date_boro = taxiFile.map(lambda line: line.split(",")).map(date_boro_mapper)

# reduce each mapped tuple by borough and time frame, then group by time frame so that we will have combined
# data for only one day
taxi_d_b_group = taxi_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])

# sort the tuples by time frame (there should be 6 elements here hence not an issue)
taxi_d_b_sorted_group = taxi_d_b_group.sortBy(lambda x: x[0])

# map the tuples to an array format to make the conversion to dataframe easy
taxi_d_b_sorted_group_d_f_format = taxi_d_b_sorted_group.map(date_boro_aggr)

# convert out RDD to dataframe for the plotting, 2014-01-01 is introduce artificially to make the ploting work
taxi_visu_data_frame = pd.DataFrame(taxi_d_b_sorted_group_d_f_format.collect(),index = [ datetime.strptime("2014-01-01 "+ item[0], "%Y-%m-%d %H:%M:%S") for item in taxi_d_b_sorted_group.collect()])

# create the data frame for the given ranges with given frequencies
taxi_datetime_index = pd.date_range(start_date, periods= n_of_periods, freq=freq)
taxi_df = pd.DataFrame(index=taxi_datetime_index)
# add the values of the RDD to the dataframe and fill zero in empty space 
taxi_df = taxi_df.add(taxi_visu_data_frame, fill_value = 0).fillna(0)
# print the output to stdout to make sure everything is as expected
pprint(taxi_df)

# create the html file for visualization 
graph(taxi_df,len(taxi_df),freq,'Day distribution for Taxi per borough', start_date+" "+ start_time)


#------Uber Analysis

# read the csv file
uberFile = sc.textFile("/uber_combined.csv")

# map each entry to ((timeFrame, borough integer representaion),1)
uber_date_boro = uberFile.map(lambda line: line.split(",")).map(date_boro_mapper)

# reduce each mapped tuple by borough and time frame, then group by time frame so that we will have combined
# data for only one day
uber_d_b_group = uber_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])

# sort the tuples by time frame (there should be 6 elements here hence not an issue)
uber_d_b_sorted_group = uber_d_b_group.sortBy(lambda x: x[0])

# map the tuples to an array format to make the conversion to dataframe easy
uber_d_b_sorted_group_d_f_format = uber_d_b_sorted_group.map(date_boro_aggr)

# convert out RDD to dataframe for the plotting, 2014-01-01 is introduce artificially to make the ploting work
uber_visu_data_frame = pd.DataFrame(uber_d_b_sorted_group_d_f_format.collect(),index = [ datetime.strptime("2014-01-01 "+ item[0], "%Y-%m-%d %H:%M:%S") for item in uber_d_b_sorted_group.collect()])

# create the data frame for the given ranges with given frequencies
uber_datetime_index = pd.date_range(start_date, periods= n_of_periods, freq=freq)
uber_df = pd.DataFrame(index=uber_datetime_index)
# add the values of the RDD to the dataframe and fill zero in empty space 
uber_df = uber_df.add(uber_visu_data_frame, fill_value = 0).fillna(0)
# print the output to stdout to make sure everything is as expected
pprint(uber_df) 

# create the html file for visualization 
graph(uber_df,len(uber_df),freq,'Day distribution for Uber per borough', start_date+" "+ start_time)
