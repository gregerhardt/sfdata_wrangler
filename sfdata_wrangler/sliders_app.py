"""
This file demonstrates a bokeh applet, which can be viewed directly
on a bokeh-server. See the README.md file in this directory for
instructions on running.
"""

import logging

logging.basicConfig(level=logging.DEBUG)

import numpy as np
import pandas as pd

from bokeh.plotting import figure
from bokeh.models import Plot, ColumnDataSource
from bokeh.properties import Instance
from bokeh.server.app import bokeh_app
from bokeh.server.utils.plugins import object_page
from bokeh.models.widgets import VBox, Slider, VBoxForm

import Visualizer
from HwyNetwork import HwyNetwork

# global parameters
INPUT_DYNAMEQ_NET_DIR    = "C:/CASA/Data/network/dynameq/validation2010.july19_Sig/Reports/Export"
INPUT_DYNAMEQ_NET_PREFIX = "pb_july19_830p"
TAXI_OUTFILE = "C:/CASA/DataExploration/taxi.h5"     
LOGGING_DIR = "C:/CASA/DataExploration"

# these store the totality of the data
link_df = None


def prepareLinkData(date='2013-02-13'):
    """ 
    Stores a dataframe (link_df) with one record for each link, containing 
    the data necessary for plotting. 
    
    Called once at the beginning to read in all the data. 
    
    date - string for the date's data to display
    """
    
    # read the highway network
    hwynet = HwyNetwork()
    hwynet.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX, logging_dir=LOGGING_DIR) 

    # start with the network links as a dataframe
    df = hwynet.getRoadLinkDataFrame()
    
    # now get the link speeds for the date, and for each hour
    store = pd.HDFStore(TAXI_OUTFILE)

    for hour in range(0,24):
        h = str(hour)
            
        # get the data
        query = "date==Timestamp('" + date + "') and hour==" + h
        obs_df = store.select('link_tt', where=query) 

        # append the hour to the end of each column name for this query
        obs_df.drop('date', axis=1, inplace=True)
        obs_df.rename(columns=lambda x: x+h, inplace=True)

        # merge, keeping all links
        df = pd.merge(df, obs_df, how='left', 
                      left_on=['ID'], right_on=['link_id'+h])
                        
        """ Calculations start here """
        # there are zero observations if its not in the righthand database
        df['observations'+h].replace(to_replace=np.nan, value=0, inplace=True)
        
        # calculate some extra fields
        length_tt_fftt = pd.Series(zip(df['LENGTH'], df['tt_mean'+h], df['FFTIME']))
        df['speed'+h] = length_tt_fftt.apply(Visualizer.calculateSpeed)
            
        tt_fftt = pd.Series(zip(df['tt_mean'+h], df['FFTIME']))
        df['tt_ratio'+h] = tt_fftt.apply(Visualizer.calculateTravelTimeRatio)
            
        # map the link colors based on the travel time ratio
        df['color'+h] = df['tt_ratio'+h].apply(Visualizer.getLinkTTRatioColor)
                
    store.close()

    df['color'] = df['color0']
    
    link_df = df

def getData(hour):
    """
    Returns a column data source with the data specific to the hour
    specified.
    
    hour - 0 to 23 to specify the hour of interest. 
    """    
    if (link_df == None):
        prepareLinkData()
    
    h = str(hour)
    df = link_df['X', 
                 'Y', 
                 'LANES', 
                 'FFSPEED', 
                 'observations'+h, 
                 'speed'+h, 
                 'color'+h
                 ]
    source = ColumnDataSource(df)
    return source       
            

class NetworkSliderApp(VBox):
    """An example of a browser-based, interactive plot with slider controls."""

    extra_generated_classes = [["NetworkSliderApp", "NetworkSliderApp", "VBox"]]

    inputs = Instance(VBoxForm)

    hour = Instance(Slider)
    
    plot = Instance(Plot)
    source = Instance(ColumnDataSource)

    @classmethod
    def create(cls):
        """One-time creation of app's objects.

        This function is called once, and is responsible for
        creating all objects (plots, datasources, etc)
        """
        obj = cls()

        obj.source = ColumnDataSource(data=dict(x=[], y=[], lanes=[], color=[]))

        obj.hour = Slider(
            title="Time of Day", name="hour",
            value=0, start=0, end=23, step=1
        )

        # Generate a figure container
        # TODO - add box_zoom tool back in when bokeh makes it work
        #        without distorting the proportions
        plot = figure(plot_width=900, # in units of px
                      plot_height=900,              
                      x_axis_type=None, 
                      y_axis_type=None,
                      tools="pan,wheel_zoom,reset,hover,save", 
                      title="SF Taxi Speeds") 
                      
        # plot the links
        plot.multi_line(xs='x', 
                        ys='y', 
                        line_width='lanes',  
                        line_color='color', 
                        source=obj.source)    
                        
        obj.plot = plot
        obj.update_data()

        obj.inputs = VBoxForm(children=[obj.hour])

        obj.children.append(obj.plot)
        obj.children.append(obj.inputs)

        return obj

    def setup_events(self):
        """Attaches the on_change event to the value property of the widget.

        The callback is set to the input_change method of this app.
        """
        super(NetworkSliderApp, self).setup_events()
        if not self.hour:
            return

        # Slider event registration
        for w in ["hour"]:
            getattr(self, w).on_change('value', self, 'input_change')
            

    def input_change(self, obj, attrname, old, new):
        """Executes whenever the input form changes.

        It is responsible for updating the plot, or anything else you want.

        Args:
            obj : the object that changed
            attrname : the attr that changed
            old : old value of attr
            new : new value of attr
        """
        self.update_data()
        

    def update_data(self):
        """Called each time that any watched property changes.

        This updates the sin wave data with the most recent values of the
        sliders. This is stored as two numpy arrays in a dict into the app's
        data source property.
        """

        self.source.data = getData(self.hour.value)
    

"""
To view this applet directly from a bokeh server, you need to run a 
bokeh-server and point it at the script.  First navigate to a directory
where it is ok for bokeh to write temporary files, then call: 

    bokeh-server --script NetworkSlidersApp.py

Now navigate to the following URL in a browser:

    http://localhost:5006/bokeh/sf
"""
@bokeh_app.route("/bokeh/sf/")
@object_page("NetworkSlider")
def make_sliders():
    app = NetworkSliderApp.create()
    return app
