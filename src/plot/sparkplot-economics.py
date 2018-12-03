from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from dateutil.parser import parse
from operator import add
import pyspark
import sys
import requests
import pandas as pd
from pprint import pprint
"""
Ride per MONTH
spark-submit sparkplot-economics.py
"""

conf = SparkConf()
conf.setAppName("Taxi_vs_Uber")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext (sc)
# read the csv file
uberFileWithNoHeader = sc.textFile("/uber_clean_sample.csv")
taxiFileWithNoHeader = sc.textFile("/taxi_clean_sample.csv")


#------ Note: we're mapping the pickup date to 2 because we know that the data we're parsing is like only 50% of all uber rides in NYC.

# takes a triplet in this format (pickup_date,pickup_time,borough) and returns a tuple in the following format
#(pickup_date, value) where the pickup date only contain the year and the month
def mapper(x,value):
	# parse the date
	date = parse(x[0])
	# extract the year and the month and map them to 2
	return(str(date.year) + "-" + str(date.month),value)

# for each line in the Uber file, this line will map the pickup_date to 2, where the pickup date only contains the month and the year.
uber_temp = uberFileWithNoHeader.map(lambda line: line.split(",")).map(lambda x : mapper(x,2))
taxi_temp = taxiFileWithNoHeader.map(lambda line: line.split(",")).map(lambda x : mapper(x,1))

# reduce by key to get the total number of rides in NYC in a month
uberData= uber_temp.reduceByKey(add)
taxiData= taxi_temp.reduceByKey(add)

# remove this line before submitting the code.
print("Number of uber rides per month")
print(uberData.collect())

print("Number of taxi rides per month")
print(taxiData.collect())

taxi_data_frame = pd.DataFrame(taxiData.collect(),  columns = ['Month','count'])

uber_data_frame = pd.DataFrame(uberData.collect(), columns = ['Month','count'])

taxi_data_frame['key'] = 'taxi'
uber_data_frame['key'] = 'uber'


if (len(taxi_data_frame['Month']) > len(uber_data_frame['Month'])):
    month_series = taxi_data_frame['Month']
else:
    month_series = uber_data_frame['Month']


combined = pd.concat([taxi_data_frame,uber_data_frame],keys=['taxi','uber'])
combined_group = combined.groupby(['Month','key'])
df_plot = combined_group.sum().unstack('key').plot(kind='line',title='Trend of taxi/uber rides per month' )

fig = df_plot.get_figure()
fig.savefig('./Aggregated Count per month.pdf')
