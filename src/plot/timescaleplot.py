import os
import json
import folium
import numpy as np
import pandas as pd
from folium.plugins import TimeSliderChoropleth
from pprint import pprint
from branca.colormap import linear

def graph(data_frame,n_periods,freq,name,start_day):
    """
    input : data_frame :: pandas data_frame of data, n_periods :: int, freq :: string, name :: string, start_day :: string
        data_frame must be in followign format aggreagted values for borough(0:4) for every row,
        each row is seperated by freq.
        n_periods : number of rows
        freq : Time gap between each period in format {'%nH','%nM','%nD'} where %n can be integer value specifying scalar multiple of hour/day/month
        name : format of %name.html that will save result map
        start_time : format of "YYYY-MM-DD" to mark start date of period
    saves the map
    out : None
    """
    #f = folium.element.Figure()
    #f.html.add_child(folium.element.Element("<h1>"+ name+"</h1>"))
    #m : folium map ojbect that has start point at newyork.a
    m = folium.Map(location=[40.718, -73.98], zoom_start=10)
    folium.TileLayer('cartodbpositron').add_to(m)
    #print(data_df)
    #df['id'] = pandas.to_numeric(df['id'])
    #df['count'] = pandas.to_numeric(df['count'])

    #create index for the time
    # num_of_periods = number of days we sample  * 4
    # for now i set it to whatever the testing dataframe is
    # freq = Frequency of sampling
    datetime_index = pd.date_range(start_day, periods= n_periods , freq=freq)
    dt_index = datetime_index.astype(int).astype('U10')
    styledata = {}
    """
    borough title :
    0 is Staten Island
    1 is Queens
    2 is Brooklyn
    3 is manhatan
    4 is Bronx
    """
    for borough in range(0,4):
        df = pd.DataFrame({'color': data_frame.iloc[:,borough].values,'opacity': data_frame.iloc[:,borough].values},index=dt_index)
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
        data['opacity'] = data['opacity']

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
    title_statement = "<h1>"+ name+"</h1>"
    m.get_root().html.add_child(folium.Element(title_statement))
    colormap = linear.OrRd_08.scale(min_color, max_color)
    colormap.caption = 'Ride count in New York boroughs'
    colormap.add_to(m)
    m.save(name+".html")
#function for normalizing the opacity
def norm(x):
    return (x - x.min()) / (x.max() - x.min())

def main():
    #data = input data for each borough[0:4] i have aggregated data for every 6 hours
    data = np.array([[0.01,0.1,0.09,0.75,0.05] ,[0.1,0.09,0.75,0.05,0.01]])
    #dataframe that will be used for plotting
    data_df = pd.DataFrame(data)
    print("testing if graph plot function is working\n result plot should be in Test-Result.html")

    graph(data_df,2,'6H',"Test-Result",'2014-1-1')
if __name__ == "__main__":

    main()
