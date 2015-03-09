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

import math
import pandas as pd
import numpy as np
import bokeh.plotting as bk

from bokeh.models import HoverTool
from bokeh.models.sources import ColumnDataSource
from dta.RoadLink import RoadLink


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

def getLinkTTRatioColor(tt_ratio):
    """
    Applies a color ramp to the travel time ratio for display. 
    
    Returns color. 
    """    
    
    # Specifies the color to use when mapping with a given travel time ratio
    colorMap = {0.00: 'Green',
                0.25: 'GreenYellow', 
                0.50: 'GreenYellow', 
                0.75: 'GreenYellow',

                1.00: '#DCDCDC',  

                1.25: '#fdd49e',  

                1.50: '#fdbb84',  

                1.75: '#fc8d59',  
                 
                2.00: '#ef6548',  
                2.25: '#ef6548',  

                2.50: '#d7301f', 
                2.75: '#d7301f', 

                3.00: '#b30000', 
                3.25: '#b30000', 

                3.50: '#7f0000',
                3.75: '#7f0000'}

    tt_ratio_floor = np.floor(tt_ratio*4.0) / 4.0
    if tt_ratio_floor < min(colorMap.keys()): 
        tt_ratio_floor = min(colorMap.keys())
    if tt_ratio_floor > max(colorMap.keys()): 
        tt_ratio_floor = max(colorMap.keys())

    color = colorMap[tt_ratio_floor]
    
    return color


def getLinkTrajectoryColor(travelTime):
    """
    Sets the color to red if there is a valid travel time, and 
    gray otherwise.  
    """    
    if np.isfinite(travelTime):
        return 'FireBrick'
    else:
        return 'Gray'


def getTimeString(datetime):
    """
    Returns a string representation of the time, given a 
    datetime object  
    """    
    return str(datetime.time())

    
class Visualizer():
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
        

    def getSegmentRectangleData(self, df):
        """
        Converts a link dataframe into a dictionary with 
        one record for each segment (can be more than one 
        segment per link if there are shape points).
        
        This will be used for the hover tool. 

        df - a dataframe with one record for each link segment.  
        """   
        xmid = []
        ymid = []
        length = []
        width = []
        angle = []

        link_id = []
        label = []
        ffspeed = []
        speed = []
        observations = []
        
        # one record for each midpoint
        for i, row in df.iterrows(): 
            xvals = row['X']
            yvals = row['Y']

            for (x1, x2, y1, y2) \
                in zip(xvals[:-1], xvals[1:], yvals[:-1], yvals[1:]):
                
                xmid.append((x1+x2)/2.0)
                ymid.append((y1+y2)/2.0)
                
                length.append(math.sqrt(((x1-x2)**2) + ((y1-y2)**2)))
                width.append(row['LANES'] * RoadLink.DEFAULT_LANE_WIDTH * 1.5)
                
                # calculate angle in radians
                angle.append(math.atan2(y2-y1, x2-x1) + np.pi/2.0)

                link_id.append(row['ID'])
                label.append(row['LABEL'])
                ffspeed.append(row['FFSPEED'])
                speed.append(row['speed'])
                observations.append(row['observations'])
        
        data=dict(xmid=xmid,
                  ymid=ymid, 
                  length=length, 
                  width=width,
                  angle=angle,
                  link_id=link_id, 
                  label=label, 
                  ffspeed=ffspeed, 
                  speed=speed,
                  observations=observations)
        
        return data

    def getTrajectoryLinkMidpointDf(self, df):
        
        """
        Converts a link dataframe into a dataframe with one 
        record at the midpoint of each link in the trajectory. 

        df - a dataframe with one record for each link. 
        """   
        
        df2 = df[np.isfinite(df['travel_time'])]
        
        x = []
        y = []
        angle = []
        text = []        
        for i, row in df2.iterrows(): 
            xvals = row['X']
            yvals = row['Y']            
            xmid = (xvals[0] + xvals[-1]) / 2.0 
            ymid = (yvals[0] + yvals[-1]) / 2.0 
            
            # calculate angle in radians
            deltax = xvals[-1] - xvals[0]
            deltay = yvals[-1] - yvals[0]
            a = math.atan2(deltay, deltax) + np.pi
            
            x.append(xmid)
            y.append(ymid)
            text.append(str(int(round(row['travel_time']))))
            angle.append(a)
        
        df3 = pd.DataFrame({'x':x, 'y':y, 'angle':angle, 'text':text})
        return df3
    
    def plotTrajectories(self, html_outfile, trajSpecs):
        """
        Prints validation plots for the trajectories specified. 
        
        html_outfile - the file to write to. 
        trajSpecs - list of tuples in the form: 
                    (date, cab_id, trip_id)
                    where all three are strings  
        """

        # setup
        bk.output_file(html_outfile, title="Trajectory Validation")
        store = pd.HDFStore(self.hdffile)
        net_df = self.hwynet.getRoadLinkDataFrame()

        plots = []
        for date, cab_id, trip_id in trajSpecs:
            
            # get the data for this case
            query = "date==Timestamp('" + date + "') and cab_id==" + cab_id \
                    + " and trip_id==" + trip_id
            point_df = store.select('trip_points', where=query) 
            traj_df = store.select('trajectories', where=query)

            # join trajectory data to network, and set the color
            df = pd.merge(net_df, traj_df, how='left', left_on=['ID'], right_on=['link_id'])
            df['color'] = df['travel_time'].apply(getLinkTrajectoryColor)

            # define the ranges, be sure to keep it square
            # to avoid distortion
            if (len(point_df)>0):
                x_extent = max(point_df['x']) - min(point_df['x'])
                y_extent = max(point_df['y']) - min(point_df['y'])
                extent = 1.3 * max(x_extent, y_extent)
    
                x_mid = (min(point_df['x']) + max(point_df['x'])) / 2.0
                y_mid = (min(point_df['y']) + max(point_df['y'])) / 2.0
                
                x_range = [x_mid - extent/2.0, x_mid + extent/2.0]
                y_range = [y_mid - extent/2.0, y_mid + extent/2.0]
            else:
                x_range = None
                y_range = None

            # generate lables
            point_df['text'] = point_df['time'].apply(getTimeString)
            traj_mid_df = self.getTrajectoryLinkMidpointDf(df)

            # set up the plot
            # TODO - add box_zoom tool back in when bokeh makes it work
            #        without distorting the proportions
            p = bk.figure(plot_width=800, # in units of px
                        plot_height=800,              
                        x_axis_type=None, 
                        y_axis_type=None,
                        x_range=x_range,
                        y_range=y_range,
                        tools="pan,wheel_zoom,reset,save", 
                        title="Taxi Trajectory:\n" 
                              + ' date=' + date 
                              + ' cab_id=' + cab_id
                              + ' trip_id=' + trip_id, 
                        title_text_font_size='14pt'
                        )    
            
            # plot the links
            p.multi_line(xs=df['X'], 
                        ys=df['Y'], 
                        line_width=df['LANES'],  
                        line_color=df['color']) 
            
            p.text(traj_mid_df['x'], 
                   traj_mid_df['y'], 
                   traj_mid_df['text'],
                   angle=traj_mid_df['angle'], 
                   text_font_size='8pt', 
                   text_align='center', 
                   text_baseline='bottom', 
                   text_color='firebrick')
            
            
            # plot the points     
            p.circle(point_df['x'], 
                     point_df['y'], 
                     fill_color='darkblue', 
                     line_color='darkblue')

            p.text(point_df['x'], 
                   point_df['y'], 
                   point_df['text'], 
                   text_font_size='8pt', 
                   text_align='center', 
                   text_baseline='bottom', 
                   text_color='darkblue')
                     
            plots.append([p])
            
        # add them all together in a grid plot
        gp = bk.gridplot(plots)
        bk.show(gp)
                        
        store.close()


    def createNetworkPlot(self, html_outfile, date='2013-02-13', hour='17'):
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
        obs_df = store.select('link_tt', where=query) 
        store.close()
        
        # merge, keeping all links
        df = pd.merge(net_df, obs_df, how='left', left_on=['ID'], right_on=['link_id'])
        
        """ Calculations start here """ 
        
        # there are zero observations if its not in the righthand database
        df['observations'].replace(to_replace=np.nan, value=0, inplace=True)
        
        # calculate some extra fields
        length_tt_fftt = pd.Series(zip(df['LENGTH'], df['tt_mean'], df['FFTIME']))
        df['speed'] = length_tt_fftt.apply(calculateSpeed)
        
        tt_fftt = pd.Series(zip(df['tt_mean'], df['FFTIME']))
        df['tt_ratio'] = tt_fftt.apply(calculateTravelTimeRatio)
        
        # map the link colors based on the travel time ratio
        df['color'] = df['tt_ratio'].apply(getLinkTTRatioColor)
        
        # TODO - fix/remove this when bokeh makes hover tool work for lines
        # see: https://github.com/bokeh/bokeh/issues/984
        segmentData = self.getSegmentRectangleData(df)

        
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
        # see: https://github.com/bokeh/bokeh/issues/2031       
        p.rect(x=segmentData['xmid'], 
               y=segmentData['ymid'], 
               height=segmentData['length'], 
               width=segmentData['width'], 
               angle=segmentData['angle'], 
               source=ColumnDataSource(segmentData),
               line_alpha=0,
               fill_alpha=0)

        hover =p.select(dict(type=HoverTool))
        hover.tooltips = OrderedDict([
            ("ID", "@link_id"),
            ("LABEL", "@label"),
            ("FFSPEED", "@ffspeed"),
            ("SPEED", "@speed"),
            ("OBSERVATIONS", "@observations")
        ])

        # plot the links
        p.multi_line(xs=df['X'], 
                     ys=df['Y'], 
                     line_width=df['LANES'],  
                     line_color=df['color'])      
                     
        # write to file and show
        bk.show(p)
