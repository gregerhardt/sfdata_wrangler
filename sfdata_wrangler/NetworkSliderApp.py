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
from bokeh.models import Plot, ColumnDataSource, MultiLine
from bokeh.properties import Instance
from bokeh.server.app import bokeh_app
from bokeh.server.utils.plugins import object_page
from bokeh.models.widgets import VBox, Slider, VBoxForm

from Visualizer import Visualizer
from HwyNetwork import HwyNetwork

# global parameters
INPUT_DYNAMEQ_NET_DIR    = "C:/CASA/Data/network/dynameq/validation2010.july19_Sig/Reports/Export"
INPUT_DYNAMEQ_NET_PREFIX = "pb_july19_830p"
TAXI_OUTFILE = "C:/CASA/DataExploration/taxi.h5"     
LOGGING_DIR = "C:/CASA/DataExploration"

class NetworkSliderApp(VBox):
    """An example of a browser-based, interactive plot with slider controls."""

    extra_generated_classes = [["NetworkSliderApp", "NetworkSliderApp", "VBox"]]

    inputs = Instance(VBoxForm)

    hour = Instance(Slider)
    
    plot = Instance(Plot)

    allLinkData = Instance(ColumnDataSource)
    selectedLinkData = Instance(ColumnDataSource)


    @classmethod
    def create(cls):
        """One-time creation of app's objects.

        This function is called once, and is responsible for
        creating all objects (plots, datasources, etc)
        """
        obj = cls()

        obj.allLinkData = ColumnDataSource(data=dict(X=[], 
                                                          Y=[], 
                                                          LANES=[], 
                                                          FFSPEED=[],
                                                          observations=[],
                                                          speed=[],
                                                          color=[]))
        
        obj.selectedLinkData = ColumnDataSource(data=dict(X=[], 
                                                          Y=[], 
                                                          LANES=[], 
                                                          FFSPEED=[],
                                                          observations=[],
                                                          speed=[],
                                                          color=[]))
        
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
        plot.multi_line(xs='X', 
                        ys='Y', 
                        line_width='LANES',  
                        line_color='color', 
                        source=obj.selectedLinkData)    
                        
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

        select the appropriate columns for this hour
        """
        if len(self.allLinkData.data['X']) == 0:
            self.allLinkData.data = self.prepareLinkData()
            #self.selectedLinkData.data = dict(X=self.allLinkData.data['X'], 
            #                              Y=self.allLinkData.data['Y'], 
            #                              LANES=self.allLinkData.data['LANES'], 
            #                              color=self.allLinkData.data['color'])
        
        h = str(self.hour.value)
        colorString = 'color' + h
        self.selectedLinkData.data = dict(X=self.allLinkData.data['X'], 
                                          Y=self.allLinkData.data['Y'], 
                                          LANES=self.allLinkData.data['LANES'], 
                                          color=self.allLinkData.data[colorString])

        self.plot.title = "SF Taxi Speeds for Hour: " + h + ":00"

        print 'updating to hour ' + h
            

    def prepareLinkData(self, date='2009-02-13'):
        """ 
        Reads and returns a dictionary with one record for each link, containing 
        the data necessary for plotting. 
        
        Called once at the beginning to read in all the data. 
        
        date - string for the date's data to display
        """
        
        # read the highway network
        hwynet = HwyNetwork()
        hwynet.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX, logging_dir=LOGGING_DIR) 
    
        # get the data
        v = Visualizer(hwynet, TAXI_OUTFILE)
        df = v.getLinkData(date=date)
        
        # convert to a dictionary.  
        # .to_dict() returns in a different structure that doesn't work. 
        d = {}
        for c in df.columns: 
            d[c] = df[c]
                
        return d


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
