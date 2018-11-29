from datetime import datetime
import reverse_geocoder as rg
import argparse
import csv
import sys
import time


# parser to parse the command line inputs
parser = argparse.ArgumentParser()
parser.add_argument("TaxiFile", help="path to the file that contains the uber data")

args = parser.parse_args()

taxiData = csv.DictReader(open(args.TaxiFile), delimiter=',')

lookUpData = { "Manhattan":"Manhattan" ,"Staten Island":"Staten Island", "Queens County":"Queens",  "Brooklyn":"Brooklyn" ,"The Bronx":"Bronx"}

print("Pickup_year,Pickup_time,Pickup_borough")
for row in taxiData:
    date = datetime.strptime(row[" pickup_datetime"],"%Y-%m-%d %H:%M:%S")
    if  (date >= datetime(2014,4,1) and date < datetime(2014,7,1)) or  (date >= datetime(2015,4,1) and date < datetime(2015,7,1)):
        pickUpLong=row[" pickup_longitude"]
        pickUpLat=row[" pickup_latitude"]
        pickupDate = row[" pickup_datetime"].split()
        if float(pickUpLong) < -60 and float(pickUpLat) > 30:
            pickUpYear=pickupDate[0]
            pickUpTime=pickupDate[1]

            data = rg.search((pickUpLat,pickUpLong),verbose=False)[0]
            borough=lookUpData.get(data["name"],"Unknown")
            if borough == "Unkown":
                borough=lookUpData.get(data["admin2"],"Unknown")                
            if borough != "Unknown":
                print(pickUpYear+","+pickUpTime+","+ borough)


