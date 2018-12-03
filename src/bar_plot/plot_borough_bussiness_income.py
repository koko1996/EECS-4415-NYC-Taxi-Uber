##########################################
# EECS4415 Assignment 1                  #
# Filename: dstat.py                     #
# Author: NANAH JI, KOKO                 #
# Email: koko96@my.yorku.com             #
# eecs_num: 215168057                    #
##########################################

import sys
import argparse
import matplotlib.pyplot as plt
from ast import literal_eval as make_tuple

# parser to parse the command line inputs
parser = argparse.ArgumentParser()
parser.add_argument("fileName", help="path to the file that contains the data")
args = parser.parse_args()

data = open(args.fileName, "r")

results = []

for row in data:
    results.append(make_tuple(row))

plt.bar(range(len(results)), [val[1] for val in results], align='center')
plt.xticks(range(len(results)), [val[0] for val in results])
plt.xticks(rotation=20,fontsize=8)
plt.show()
