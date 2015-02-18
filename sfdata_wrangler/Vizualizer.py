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

import pandas as pd
import numpy as np
import bokeh.plotting as bk


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
    colorMap = {0.0: 'Green',
                0.2: 'GreenYellow', 
                0.4: 'GreenYellow', 
                0.6: 'GreenYellow', 
                0.8: 'GreenYellow', 
                1.0: '#fff7ec',  
                1.2: '#fee8c8',  
                1.4: '#fdd49e',  
                1.6: '#fdbb84',  
                1.8: '#fc8d59',  
                2.0: '#ef6548',  
                2.2: '#d7301f', 
                2.4: '#b30000', 
                2.6: '#7f0000'}

    tt_ratio_floor = np.floor(tt_ratio*5.0) / 5.0
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
        
        

    def createNetworkPlots(self, html_outfile, inkey):
        """ 
        Creates network plots showing the link speeds. 
        
        html_outfile - the file to write to. 
        inkey - the key in the store to find the dataframe of interest
         
        """
        
        # start with the network links as a dataframe
        net_df = self.hwynet.getRoadLinkDataFrame()
        
        # now get the link speeds for the first date
        # and for 5-6 pm
        store = pd.HDFStore(self.hdffile)
        dates = store.select_column(inkey, 'date').unique()
        dates.sort()
        date = dates[0]
        obs_df = store.select(inkey, where='date==Timestamp(date) and hour=17') 
        store.close()
        
        # merge, keeping all links
        df = pd.merge(net_df, obs_df, how='left', left_on=['ID'], right_on=['link_id'])
        
        # there are zero observations if its not in the right database
        df['observations'].replace(to_replace=np.nan, value=0, inplace=True)
        
        # calculate some extra fields
        length_tt_fftt = pd.Series(zip(df['LENGTH'], df['tt_mean'], df['FFTIME']))
        df['speed'] = length_tt_fftt.apply(calculateSpeed)
        
        tt_fftt = pd.Series(zip(df['tt_mean'], df['FFTIME']))
        df['tt_ratio'] = tt_fftt.apply(calculateTravelTimeRatio)
        
        # map the link colors based on the travel time ratio
        df['color'] = df['tt_ratio'].apply(getLinkColor)
        
        # specify the output file
        bk.output_file(html_outfile, title="Taxi Speed Analysis")
        
        # set up the plot
        # TODO - add box_zoom tool back in when bokeh makes it work
        #        without distorting the proportions
        p = bk.figure(plot_width=900, # in units of px
                      plot_height=900,              
                      x_axis_type=None, 
                      y_axis_type=None,
                      tools="pan,wheel_zoom,reset,hover,save", 
                      title="San Francisco street network")      
        
        # plot the links
        p.multi_line(xs=df['X'], 
                     ys=df['Y'], 
                     line_width=df['LANES'],  
                     line_color=df['color'])

        # write to file and show
        bk.show(p)
