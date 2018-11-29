import reverse_geocoder as rg
from datetime import datetime
import argparse
import csv
import sys
import time


# parser to parse the command line inputs
parser = argparse.ArgumentParser()
parser.add_argument("lookupFile", help="path to the file that contains the conversion data")
parser.add_argument("uberData", help="path to the file that contains the uber data")

args = parser.parse_args()

lookUpData = csv.DictReader(open(args.lookupFile), delimiter=',')
lookUp = []
lookUp.append("Unknown")
for row in lookUpData:
    lookUp.append(row["Borough"])

# close csv lookupData
uberData = csv.DictReader(open(args.uberData), delimiter=',')

print("Pickup_year,Pickup_time,Pickup_borough")
for row in uberData:
    date = datetime.strptime(row["Pickup_date"],"%Y-%m-%d %H:%M:%S")
    if  (date >= datetime(2014,4,1) and date < datetime(2014,7,1)) or  (date >= datetime(2015,4,1) and date < datetime(2015,7,1)):
        pickupDate = row["Pickup_date"].split()
        pickUpYear=pickupDate[0]
        pickUpTime=pickupDate[1]
        borough=lookUp[int(row["locationID"])]
        if borough != "Unknown" :
            print(pickUpYear+","+pickUpTime+","+borough)


