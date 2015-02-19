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

from collections import OrderedDict

import pandas as pd
import numpy as np
import bokeh.plotting as bk

from bokeh.models import HoverTool
from bokeh.models.sources import ColumnDataSource


def calculateSpeed(length_tt_fftt):        
    """
    Calculates the speed in mph given a tuple of the 
    length in miles, the travel time in seconds, and the free flow travel
    time in seconds. 
    
    Deals with missing data by using the free flow speed. 
    
    Returns speed in mph
    """    
    (length, tt, fftt) = length_tt_fftt

    if (np.isnan(tt) or tt==0):
        tt = fftt
    speed = length / (tt/3600.0)

    return speed


def calculateTravelTimeRatio(tt_fftt):        
    """
    Calculates the ratio of the observed travel time to the
    free flow travel time.     
    Deals with missing data by using the free flow speed. 
    
    Returns ratio. 
    """
    (tt, fftt) = tt_fftt

    if (np.isnan(tt) or tt==0):
        tt = fftt
    ratio = tt / fftt
    
    return ratio

def getLinkColor(tt_ratio):
    """
    Applies a color ramp to the travel time ratio for display. 
    
    Returns color. 
    """    
    
    # Specifies the color to use when mapping with a given travel time ratio
    colorMap = {0.00: 'Green',
                0.25: 'GreenYellow', 
                0.50: 'GreenYellow', 
                0.75: 'GreenYellow',
                1.00: 'WhiteSmoke',  
                1.25: '#fdd49e',  
                1.50: '#fdbb84',  
                1.75: '#fc8d59',  
                2.00: '#ef6548',  
                2.25: '#d7301f', 
                2.50: '#b30000', 
                2.75: '#7f0000'}

    tt_ratio_floor = np.floor(tt_ratio*4.0) / 4.0
    if tt_ratio_floor < min(colorMap.keys()): 
        tt_ratio_floor = min(colorMap.keys())
    if tt_ratio_floor > max(colorMap.keys()): 
        tt_ratio_floor = max(colorMap.keys())

    color = colorMap[tt_ratio_floor]
    
    return color
    
    
class Vizualizer():
    """ 
    Class to vizualize the data outputs. 
    """

    
    def __init__(self, hwynet, hdffile):
        """
        Constructor. 

        hwynet - a HwyNetwork for getting network information and configuration.         
        hdffile - a hdf5 datastore file containing the processed data
        """   
        self.hwynet = hwynet
        self.hdffile = hdffile
        

    def getLinkMidpointData(self, df):
        """
        Converts a link dataframe into a dictionary with 
        one record for the midpoint of each segment (can be 
        more than one segment per link if there are shape points).
        
        This will be used for the hover tool. 

        df - a dataframe with one record for each link. 
        """   
        x = []
        y = []
        link_id = []
        label = []
        ffspeed = []
        speed = []
        observations = []
        color = []
        width = []
        
        # one record for each midpoint
        for i, row in df.iterrows(): 
            xvals = row['X']
            yvals = row['Y']
            for (x1, x2, y1, y2) \
                in zip(xvals[:-1], xvals[1:], yvals[:-1], yvals[1:]):
                
                x.append((x1 + x2) / 2.0)
                y.append((y1 + y2) / 2.0)
                link_id.append(row['ID'])
                label.append(row['LABEL'])
                ffspeed.append(row['FFSPEED'])
                speed.append(row['speed'])
                observations.append(row['observations'])
                color.append(row['color'])
                width.append(row['LANES'] * 0.9)
        
        data=dict(x=x, 
                  y=y, 
                  link_id=link_id, 
                  label=label, 
                  ffspeed=ffspeed, 
                  speed=speed,
                  observations=observations, 
                  color=color, 
                  width=width)
        
        return data
        

    def createNetworkPlot(self, html_outfile, inkey, date='2013-02-13', hour='17'):
        """ 
        Creates network plots showing the link speeds. 
        
        html_outfile - the file to write to. 
        inkey - the key in the store to find the dataframe of interest
        date - string for the date's data to display
        hour - string for the hour to query, from 0 to 23 
        """
        
        # start with the network links as a dataframe
        net_df = self.hwynet.getRoadLinkDataFrame()
        
        # now get the link speeds for the first date
        # and for 5-6 pm
        store = pd.HDFStore(self.hdffile)
        query = "date==Timestamp('" + date + "') and hour==" + hour
        obs_df = store.select(inkey, where=query) 
        store.close()
        
        # merge, keeping all links
        df = pd.merge(net_df, obs_df, how='left', left_on=['ID'], right_on=['link_id'])
        
        """ Calculations start here """ 
        
        # there are zero observations if its not in the right database
        df['observations'].replace(to_replace=np.nan, value=0, inplace=True)
        
        # calculate some extra fields
        length_tt_fftt = pd.Series(zip(df['LENGTH'], df['tt_mean'], df['FFTIME']))
        df['speed'] = length_tt_fftt.apply(calculateSpeed)
        
        tt_fftt = pd.Series(zip(df['tt_mean'], df['FFTIME']))
        df['tt_ratio'] = tt_fftt.apply(calculateTravelTimeRatio)
        
        # map the link colors based on the travel time ratio
        df['color'] = df['tt_ratio'].apply(getLinkColor)
        
        # TODO - fix/remove this when bokeh makes hover tool work for lines
        # see: https://github.com/bokeh/bokeh/issues/984
        pointData = self.getLinkMidpointData(df)

        
        """ Plotting starts here """ 
        
        # specify the output file
        bk.output_file(html_outfile, title="San Francisco Vizualization")
        
        # set up the plot
        # TODO - add box_zoom tool back in when bokeh makes it work
        #        without distorting the proportions
        p = bk.figure(plot_width=900, # in units of px
                      plot_height=900,              
                      x_axis_type=None, 
                      y_axis_type=None,
                      tools="pan,wheel_zoom,reset,hover,save", 
                      title="SF Taxi Speeds: " + date + ', hr=' + hour)      
                  
        # TODO - fix/remove this when bokeh makes hover tool work for lines
        # see: https://github.com/bokeh/bokeh/issues/984        
        p.circle(pointData['x'], 
                 pointData['y'], 
                 source=ColumnDataSource(pointData),
                 size=pointData['width'],  
                 line_color=pointData['color'],
                 fill_color=pointData['color'])

        hover =p.select(dict(type=HoverTool))
        hover.tooltips = OrderedDict([
            ("ID", "@link_id"),
            ("LABEL", "@label"),
            ("FFSPEED", "@ffspeed"),
            ("SPEED", "@speed"),
            ("OBSERVATIONS", "@observations"),
        ])

        # plot the links
        p.multi_line(xs=df['X'], 
                     ys=df['Y'], 
                     line_width=df['LANES'],  
                     line_color=df['color'])      
                     
        # write to file and show
        bk.show(p)
