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

# open the look up file that translates locationIDs to boroughs
lookUpData = csv.DictReader(open(args.lookupFile), delimiter=',')
# create a list for translation from locationIDs to boroughs
lookUp = []
lookUp.append("Unknown")
for row in lookUpData:
    lookUp.append(row["Borough"])

# close csv lookupData

# open the uber data
uberData = csv.DictReader(open(args.uberData), delimiter=',')

# print the header of the output csv
print("Pickup_year,Pickup_time,Pickup_borough")

# go through the input
for row in uberData:
    # handle every exception except KeyboardInterrupt
    try: 
        # parse the date and time to a datetime object
        date = datetime.strptime(row["Pickup_date"],"%Y-%m-%d %H:%M:%S")
        # check if the date and time is within our accepted ranges
        if  (date >= datetime(2014,4,1) and date < datetime(2014,7,1)) or  (date >= datetime(2015,4,1) and date < datetime(2015,7,1)):
            # look up the borough that is corresponding to the location id
            borough=lookUp[int(row["locationID"])]

            # remove the line if it is not in one of the boroughs
            if borough != "Unknown" :
                # get formatted year and date
                pickUpYear=date.strftime("%Y-%m-%d")
                pickUpTime=date.strftime('%H:%M:%S')
                print(pickUpYear+","+pickUpTime+","+borough)
    except KeyboardInterrupt:
        raise
    except:
        print("fatal error in " + str(row) , file=sys.stderr)     


