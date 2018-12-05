## EECS-4415-NYC-Taxi-Uber
For detailed description for this project check the "EECS 4415 Project Proposal.pdf" file 

TThis file contains instruction on how to run the scripts provided with the project

#------------------------Pre Processing

How to run the preprocessing scripts:

1- download the pre-processing folder

2- download all the raw data from 
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
https://www.kaggle.com/fivethirtyeight/uber-pickups-in-new-york-city
https://github.com/zonination/weather-us

Since the data comes in with chunks from the sources you need to run the pre-processor scripts on each chunk seperetly and after finishing the preprocessing combine all the Uber data in one file (order does not matter) and all the taxi data in another file (order does not matter)
We name the combined uber data uber_combined.csv and taxi data taxi_combined.csv which can be found in the zipped file name Combined_clean_data.zip

running the the preprocessing scripts for uber data 2014
1- please ensure that uber data is located in the same folder as the preprocessing script
2- install revers geocoder using
    " pip install reverse_geocoder"
3- use the following script:
    "python3 pre-processing-uber.py "name of the file that contains uber data 2014: > output_uber_2014.csv

4- repeat the steps 1-3 for Uber data 2015 and for NYC taxi data (you have to run the preprocessing script for each month for taxi data, and combine the results )


running the preprocessing scripts for weather data:
1- please ensure that raw weather data is located in the same folder as the preprocessing script
2- use the following script:
    " python3 pre-processing-weather.py < "name of the file that contain weather data" > output_weather.csv

    

#------------------------analysis

1- start a docker container and mount the directory with all the cleaned data, you can find a copy of cleaned data in the provide zip
to start a docker container and mount the directory use
    "docker run -it -v  "$PWD:/app" -w  "/app"  eecsyorku/eecs4415"
once the container is started upload the all processed and clean data to hdfs using the following command as an example:
    "hdfs dfs -put uber_combined.csv  /"  

we use an external modules, use the following commands to install them:
    "pip install python-dateutil"
    "pip install pandas"
and feel free to install any other module that your docker image does not have and the interpreter complains that you don't have while running the scripts

We have two sort of analysis done, one that prints the output to a file in the hdfs and the other one generates an html file to make visualisation easier

now to run any of the analysis scripts with the following command:
    for example to run Economics.py 
    "spark-submit Economics.py"

-The analysis scripts that generate a file are in the "analysis" folder under src dir, to visualize the results of these analysis you need to move the combined result of this script to local file system and run the plot_borough_bussiness_income.py script in the bar_plot dir passing the resutls filepath as the argument
such as:
    "python plot_borough_bussiness_income.py analysed_data_combined/uber_economics_processed_combined"

as long as the all cleaned data are in HDFS and the required modules have been installed correctly, the scripts will run without any issue. inside the scripts we save the output to a txt file, so you don't have to output directing. However, the output will be divided to multiple files that start with port-00**** by the spark that is running on hdfs so you should get the output directory that was generated after you ran the spark-submit commmand and add the output files to a single file 
one way to do this is:
    "cat port-000000 >> results.out"
                .
                .
                .                                                
    "cat port-00000n >> results.out"
for all the port-000*** files

-The analysis scripts that generate html files are under analysis_html where we have two scripts sparkplot-residential-analysis.py and sparkplot-time-space-analysis.py both of these generate html files that should work on any browser (FYI: we have only tested it on firefox)




    