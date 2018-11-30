from __future__ import print_function
from datetime import datetime
import reverse_geocoder as rg
import argparse
import csv
import sys
import time


# parser to parse the command line inputs
parser = argparse.ArgumentParser()
parser.add_argument("TaxiFile", help="path to the file that contains the taxi data")
args = parser.parse_args()

# look up dictonary to convert the output of reverse_geocoder to one unified name across the four preprocessing scripts
# since in the other script we are using another way to convert the values to boroughs based on the input of that file
lookUpData = { "Manhattan":"Manhattan" ,"Staten Island":"Staten Island", "Queens County":"Queens",  "Brooklyn":"Brooklyn" ,"The Bronx":"Bronx"}

# open the Taxi data
taxiData = csv.DictReader(open(args.TaxiFile), delimiter=',')

# print the header of the output csv
print("Pickup_year,Pickup_time,Pickup_borough")

# go through the input
for row in taxiData:
    # handle every exception except KeyboardInterrupt
    try:
        # parse the date and time to a datetime object
        date = datetime.strptime(row[" pickup_datetime"],"%Y-%m-%d %H:%M:%S")
        # check if the date and time is within our accepted ranges
        if  (date >= datetime(2014,4,1) and date < datetime(2014,7,1)) or  (date >= datetime(2015,4,1) and date < datetime(2015,7,1)):
            # parse the lat and long of the input 
            pickUpLong=row[" pickup_longitude"]
            pickUpLat=row[" pickup_latitude"]

            # don't try to convert the lat long to borough unless it is withing expected range
            if float(pickUpLong) < -60 and float(pickUpLat) > 30:
                
                # convert lat and long to borough
                # reverse_geocoder is little inconsistent with the values that it returnes, every borough except Queens is under data["name"] but queens is under data["admin2"] so It will first check if data["name"] is in the dictionary hence not Queens and if not then it will check if data["admin2"] is in the dictionary hence it maps to Queens                
                data = rg.search((pickUpLat,pickUpLong),verbose=False)[0]
                # check if the name field in the returned data matches any of the borough names we have in the lookup dictionary
                borough=lookUpData.get(data["name"],"Unknown")
                # if the name filed does not match the borough names we have in the lookup dictionary
                if borough == "Unkown":
                # check if the admin2 field in the returned data matches any of the borough names we have in the lookup dictionary
                    borough=lookUpData.get(data["admin2"],"Unknown")                
                
                # remove the line if it is not in one of the boroughs
                if borough != "Unknown":
                    # get formatted year and date
                    pickUpYear=date.strftime("%Y-%m-%d")
                    pickUpTime=date.strftime('%H:%M:%S')
                    print(pickUpYear+","+pickUpTime+","+ borough)
    except KeyboardInterrupt:
        raise                    
    except:
        print("fatal error in " + str(row) , file=sys.stderr)        


