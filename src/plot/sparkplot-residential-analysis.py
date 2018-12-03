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
    convert the [date,list of all ride in integer borough code]
    to [date,list of aggregated boroughs]
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

# read the csv file
taxiFileWithNoHeader = sc.textFile("/taxi_combined.csv")

# mapp each line in the input file to (pickupYear-pickupMonth,borough)
taxi_date_boro = taxiFileWithNoHeader.map(lambda line: line.split(",")).map(date_boro_mapper)

# group the mapped values by year 
taxi_d_b_group = taxi_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])

# sort the values by year 
taxi_d_b_sorted_group = taxi_d_b_group.sortBy(lambda x: x[0][0])

# aggregate by month of a year by borough 
taxi_d_b_sorted_group_d_f_format = taxi_d_b_sorted_group.map(date_boro_aggr)

#dataframe that will be used for plotting
taxi_visu_data_frame = pd.DataFrame(taxi_d_b_sorted_group_d_f_format.collect(),index = [datetime.strptime(item[0],"%Y-%m") for item in taxi_d_b_sorted_group.collect()])


taxi_datetime_index_2014 = pd.date_range('2014-04', periods= n_of_periods, freq='M')
taxi_datetime_index_2015 = pd.date_range('2015-04', periods= n_of_periods, freq='M')
taxi_datetime_index_2015.reset_index(drop=True, inplace=True)
taxi_datetime_index = pd.concat([taxi_datetime_index_2014,taxi_datetime_index_2015], axis=1)

taxi_visu = pd.DataFrame(index=taxi_datetime_index)
taxi_visu = taxi_visu.add(taxi_visu_data_frame, fill_value = 0).fillna(0)

pprint(taxi_visu)

graph(taxi_visu,len(taxi_visu),'M','Monthly count per borough for taxi','2014-04')


#------Uber Analysis

uberFileWithNoHeader = sc.textFile("/uber_combined.csv")

uber_date_boro = uberFileWithNoHeader.map(lambda line: line.split(",")).map(date_boro_mapper)
uber_d_b_group = uber_date_boro.reduceByKey(lambda x,y: x + y).groupBy(lambda x: x[0][0])
uber_d_b_sorted_group = uber_d_b_group.sortBy(lambda x: x[0][0])
uber_d_b_sorted_group_d_f_format = uber_d_b_sorted_group.map(date_boro_aggr)
uber_visu_data_frame = pd.DataFrame(uber_d_b_sorted_group_d_f_format.collect(),index = [item[0] for item in uber_d_b_sorted_group.collect()])

uber_datetime_index_2014 = pd.date_range('2014-04', periods= n_of_periods, freq='M')
uber_datetime_index_2015 = pd.date_range('2015-04', periods= n_of_periods, freq='M')
uber_datetime_index_2015.reset_index(drop=True, inplace=True)
uber_datetime_index = pd.concat([uber_datetime_index_2014,uber_datetime_index_2015], axis=1)

uber_visu = pd.DataFrame(index=uber_datetime_index)
uber_visu = uber_visu.add(uber_visu_data_frame, fill_value = 0).fillna(0)

graph(uber_visu,len(uber_visu),'M','Monthly count per borough for uber','2015-04')
