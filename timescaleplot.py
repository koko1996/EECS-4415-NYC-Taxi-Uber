import os
import json
import folium
import numpy as np
import pandas as pd
from folium.plugins import TimeSliderChoropleth
from pprint import pprint
from branca.colormap import linear
def graph():
    #m : folium map ojbect that has start point at newyork.a
    m = folium.Map(location=[40.718, -73.98], zoom_start=10)
    folium.TileLayer('cartodbpositron').add_to(m)
    #data = input data for each borough[0:4] i have aggregated data for every 6 hours
    data = np.array([[35,23,23,42,14] ,[33,22,11,15,52],[55,33,22,11,41],[11,15,33,52,27],[33,52,0,32,11],[11,83,100,9,0],[22,25,32,11,86],[1,2,3,4,5]])

    #dataframe that will be used for plotting
    data_df = pd.DataFrame(data)
    #print(data_df)
    #df['id'] = pandas.to_numeric(df['id'])
    #df['count'] = pandas.to_numeric(df['count'])

    #create index for the time
    # num_of_periods = number of days we sample  * 4
    # for now i set it to whatever the testing dataframe is
    # freq = Frequency of sampling
    n_periods = 8
    n_sample = 5
    datetime_index = pd.date_range('2014-1-1', periods= n_periods , freq='6H')
    dt_index = datetime_index.astype(int).astype('U10')
    styledata = {}
    # i need to convert input data to
    #for borough in range(0,4):
    #    df = pd.DataFrame({'color': data_df[0] , 'opacity': data_df[0]},index=dt_index)
        #df.sample(n_sample,replace =False).sort_index()
    #    styledata[borough] = df
    for borough in range(0,4):
        df = pd.DataFrame({'color': data_df.iloc[:,borough].values,'opacity': data_df.iloc[:,borough].values},index=dt_index)
        df.sort_index()
        styledata[borough] = df

        max_color, min_color, max_opacity, min_opacity = 0, 0, 0, 0

    for borough, data in styledata.items():
        max_color = max(max_color, data['color'].max())
        min_color = min(max_color, data['color'].min())
        max_opacity = max(max_color, data['opacity'].max())
        max_opacity = min(max_color, data['opacity'].max())

    cmap = linear.PuRd_09.scale(min_color, max_color)


    #convert color value to hexcolor
    #and normalize the opacity
    for country, data in styledata.items():
        data['color'] = data['color'].apply(cmap)
        data['opacity'] = norm(data['opacity'])

    styledict = {
        str(borough): data.to_dict(orient='index') for
        borough, data in styledata.items()
    }

    #load the geojson file that we have
    nyc_zone = os.path.join('./','newyorkborough.json')
    nyc_json = json.load(open(nyc_zone))

    #create timesliderchoropleth with geojson file & style as arguments and add it to the map
    g = TimeSliderChoropleth( nyc_json,  styledict=styledict).add_to(m)
    #save the map
    m.save("Result.html")
def norm(x):
    return (x - x.min()) / (x.max() - x.min())

def main():
    graph()
if __name__ == "__main__":
    main()
