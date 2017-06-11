
# allows python3 style print function
from __future__ import print_function


# -*- coding: utf-8 -*-
__author__      = "Gregory D. Erhardt"
__copyright__   = "Copyright 2013 SFCTA"
__license__     = """
    This file is part of sfdata_wrangler.

    sfdata_wrangler is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    sfdata_wrangler is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with sfdata_wrangler.  If not, see <http://www.gnu.org/licenses/>.
"""

"""
This file demonstrates a bokeh applet, which can be viewed directly
on a bokeh-server. See the end of the file for
instructions on running.
"""

import logging

logging.basicConfig(level=logging.DEBUG)

from collections import OrderedDict

from bokeh.plotting import figure
from bokeh.models import Plot, ColumnDataSource, HoverTool
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

    allSegmentData = Instance(ColumnDataSource)
    selectedSegmentData = Instance(ColumnDataSource)

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
                                                     color=[]))
        
        obj.selectedLinkData = ColumnDataSource(data=dict(X=[], 
                                                          Y=[], 
                                                          LANES=[], 
                                                          color=[]))
            
        obj.allSegmentData = ColumnDataSource(data=dict(xmid=[], 
                                                        ymid=[],
                                                        length=[],
                                                        width=[],
                                                        angle=[],
                                                        link_id=[],
                                                        label=[],
                                                        ffspeed=[],
                                                        speed=[],
                                                        observations=[]))

        obj.selectedSegmentData = ColumnDataSource(data=dict(xmid=[], 
                                                        ymid=[],
                                                        length=[],
                                                        width=[],
                                                        angle=[],
                                                        link_id=[],
                                                        label=[],
                                                        ffspeed=[],
                                                        speed=[],
                                                        observations=[]))

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
                    
        # TODO - fix/remove this when bokeh makes hover tool work for lines
        # see: https://github.com/bokeh/bokeh/issues/2031       
        plot.rect(x='xmid', 
                  y='ymid', 
                  height='length', 
                  width='width', 
                  angle='angle', 
                  source=obj.allSegmentData,
                  line_alpha=0,
                  fill_alpha=0)

        hover =plot.select(dict(type=HoverTool))
        hover.tooltips = OrderedDict([
            ("ID", "@link_id"),
            ("LABEL", "@label"),
            ("FFSPEED", "@ffspeed"),
            ("SPEED", "@speed"),
            ("OBSERVATIONS", "@observations")
        ])

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
        
        # initialize the first time through
        if len(self.allLinkData.data['X']) == 0:
            (linkData, segmentData) = self.prepareLinkData()
            self.allLinkData.data = linkData
            self.allSegmentData.data = segmentData        
        
        h = str(self.hour.value)
        
        colorString = 'color' + h
        self.selectedLinkData.data = dict(X=self.allLinkData.data['X'], 
                                          Y=self.allLinkData.data['Y'], 
                                          LANES=self.allLinkData.data['LANES'], 
                                          color=self.allLinkData.data[colorString])

        self.selectedSegmentData.data = dict(xmid=self.allSegmentData.data['xmid'], 
                                             ymid=self.allSegmentData.data['ymid'],
                                             length=self.allSegmentData.data['length'],
                                             width=self.allSegmentData.data['width'],
                                             angle=self.allSegmentData.data['angle'],
                                             link_id=self.allSegmentData.data['link_id'],
                                             label=self.allSegmentData.data['label'],
                                             ffspeed=self.allSegmentData.data['ffspeed'],
                                             speed=self.allSegmentData.data['speed'+h],
                                             observations=self.allSegmentData.data['observations'+h])

        self.plot.title = "SF Taxi Speeds for Hour: " + h + ":00"

        print 'updating to hour ' + h
            

    def prepareLinkData(self, date='2009-02-13'):
        """ 
        Reads and returns a tuple of (linkData, segmentData). 
        
        linkData is a dictionary with one record for each link, containing 
        the data necessary for plotting. 
        
        segmentData is a dictionary with one record for each shape segment
        for use with the HoverTool. 
        
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
        linkData = {}
        for c in df.columns: 
            linkData[c] = df[c]
                
        # convert to segments
        segmentData = v.getSegmentRectangleData(df)
                                
        return (linkData, segmentData)


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
