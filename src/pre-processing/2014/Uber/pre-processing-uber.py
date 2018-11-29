import reverse_geocoder as rg
from datetime import datetime
import argparse
import csv
import sys
import time


# parser to parse the command line inputs
parser = argparse.ArgumentParser()
parser.add_argument("uberData", help="path to the file that contains the uber data")
args = parser.parse_args()

lookUpData = { "Manhattan":"Manhattan" ,"Staten Island":"Staten Island", "Queens County":"Queens",  "Brooklyn":"Brooklyn" ,"The Bronx":"Bronx"}

uberData = csv.DictReader(open(args.uberData), delimiter=',')

print("Pickup_year,Pickup_time,Pickup_borough")
for row in uberData:
    date = datetime.strptime(row["Date/Time"],"%m/%d/%Y %H:%M:%S")
    if  (date >= datetime(2014,4,1) and date < datetime(2014,7,1)) or  (date >= datetime(2015,4,1) and date < datetime(2015,7,1)):
        pickupDate = row["Date/Time"].split()
        pickUpYear=pickupDate[0]
        pickUpTime=pickupDate[1]
        pickUpLong=row["Lon"]
        pickUpLat=row["Lat"]
        if float(pickUpLong) < -60 and float(pickUpLat) > 30:
            data = rg.search((pickUpLat,pickUpLong),verbose=False)[0]
            borough=lookUpData.get(data["name"],"Unknown")
            if borough == "Unkown":
                borough=lookUpData.get(data["admin2"],"Unknown")                
            if borough != "Unknown":
                print(pickUpYear+","+pickUpTime+","+ borough)
         