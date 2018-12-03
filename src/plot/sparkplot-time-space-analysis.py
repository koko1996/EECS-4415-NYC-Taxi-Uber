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
"""
RIDE PER DAY
spark-submit sparkplot-business-improvement
"""
def date_boro_mapper(x):
    """
    simply map each borough to integer representation
    borough title :
    0 is Staten Island
    1 is Queens
    2 is Brooklyn
    3 is manhatan
    4 is Bronx
    and time to 4 frames 00  06 12 18
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
    converted = x[1]
    if(x[1] <= "23:59:59"):
        converted = ' 18:00:00'
    if(x[1] <= "17:59:59"):
        converted = ' 12:00:00'
    if(x[1] <= "011:59:59"):
        converted = ' 06:00:00'
    if(x[1] <= "05:59:59"):
        converted = ' 00:00:00'
    return(x[0]+converted,value)
def date_boro_aggr(x):
    """
    convert the [date,list of all ride in integer borough code]
    to [date,list of aggregated boroughs]
    """
    temp = [0,0,0,0,0]
    for ele in x[1]:
        temp[int(ele[1])] += 1
    return temp
conf = SparkConf()
conf.setAppName("Taxi_vs_Uber")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)
# read the csv file
uberFile = sc.textFile("/uber_combined.csv")
taxiFile = sc.textFile("/taxi_combined.csv")



"""
    Map each row into timeframe {00:00:00,06:00:00,12:00:00,18:00:00} & borough {0,1,2,3,4,}
"""
# for each line in the Uber file, this line will map the pickup_date to 2, where the pickup date only contains the month and the year.
taxi_date_boro = taxiFile.map(lambda line: line.split(",")).map(date_boro_mapper)
uber_date_boro = uberFile.map(lambda line: line.split(",")).map(date_boro_mapper)

"""
    group them by day(time) and sort them by time, then format them into dataframeformat list of arrays [count for each borough].
    Then insert time as index to each dataframe

"""
taxi_d_b_group = taxi_date_boro.groupBy(lambda x: x[0])
uber_d_b_group = uber_date_boro.groupBy(lambda x: x[0])

taxi_d_b_sorted_group = taxi_d_b_group.sortBy(lambda x: x[0])
uber_d_b_sorted_group = uber_d_b_group.sortBy(lambda x: x[0])

taxi_d_b_sorted_group_d_f_format = taxi_d_b_sorted_group.map(date_boro_aggr)
uber_d_b_sorted_group_d_f_format = uber_d_b_sorted_group.map(date_boro_aggr)

#dataframe that will be used for plotting
# we index each dataframe row with time
taxi_visu_data_frame = pd.DataFrame(taxi_d_b_sorted_group_d_f_format.collect(),index = [item[0] for item in taxi_d_b_sorted_group.collect()])
uber_visu_data_frame = pd.DataFrame(uber_d_b_sorted_group_d_f_format.collect(),index = [item[0] for item in uber_d_b_sorted_group.collect()])
"""
    ====Section that you have to alter for each script (for now)====
    Number of periods refers to number of frames of map that you are displaying.
    If you want 4 months worth of Every day, you will set n_of_periods =  4 * 30 = 120.
    If you want 10 days worth of every 6 hours, you will set n_of_periods = 10 * (24/6) = 40
    Once you determine the number of periods, you want to set the start date in a format
    start_date = 'YYYY-MM--DD' and start time =  'HH:MM:SS' if you want it daily in hours
    freq - the frequency for each window eg. freq = '6H' every 6 hours you mark the time
    freq = choose 1 from {H,D,M,Y} - hour day month year and put integer value if you want to do scalar multiple of it
    freq = 6H = 6 hours 4H = 4 hours window H = 1 hour window
"""

n_of_periods = 360
start_date = '2015-04-01'
start_time = '00:00:00'
freq = '6H'
datetime_index = pd.date_range(start_date, periods= n_of_periods, freq=freq)
#create dataframe template that includes everything with fill factor fo 0 for
together = df2 = pd.DataFrame(index=datetime_index)
together = together.add(taxi_visu_data_frame, fill_value = 0)
together = together.add(uber_visu_data_frame, fill_value = 0)
together = together.fillna(0)
"""
command for actual plotting, change 4th argument to change name of the result html file.
"""
graph(together,len(together),freq,'Time analysis per borough', start_date+" "+ start_time)
