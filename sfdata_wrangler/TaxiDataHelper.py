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
import datetime
import HwyNetwork
from Trajectory import Trajectory
from mm.path_inference.structures import Position

# used to calculate number of points in each trip
def setNumPointsAndLength(df):
    df['num_points'] = len(df)
    df['trip_length'] = df['feet'].sum()
    return df                    
                                    

class TaxiDataHelper():
    """ 
    Methods used to read taxi GPS points and use them to calculate 
    link speeds. 
    """

    # number of rows to read at a time
    CHUNKSIZE = 10000
    
    # speed threshold under which vehicles are considered stationary
    #   1 mph = 88 ft/min, or about 3.5 vehicle lengths between recordings
    SPEED_THRESHOLD = 1.0  # mph
    
    # time vehicle must be stationary for the record to be dropped
    #   longer than the cycle length of the signals
    TIME_THRESHOLD = 120.0   # seconds
    
    # threshold at which an entire trip (secquence of points) is counted
    #   500 ft is about the length of a city block
    TRIP_DIST_THRESHOLD = 500

    def __init__(self):
        """
        Constructor.             
        """   
                    
    def processRawData(self, infile, outfile, outkey):
        """
        Read taxi data, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in raw CSV format
        outfile - output file name in h5 format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # set up the reader
        reader = pd.read_csv(infile,  
                         sep = '\t',
                         iterator = True, 
                         chunksize= self.CHUNKSIZE)

        # establish the writer
        store = pd.HDFStore(outfile)

        # iterate through chunk by chunk so we don't run out of memory
        rowsRead    = 0
        rowsWritten = 0
        for chunk in reader:   

            rowsRead    += len(chunk)
        
            # convert to x y coordinates
            chunk['x'] = chunk['longitude'].apply(HwyNetwork.convertLongitudeToX)
            chunk['y'] = chunk['latitude'].apply(HwyNetwork.convertLatitudeToY)
        
            # convert to timedate formats
            chunk['date'] = pd.to_datetime(chunk['time'],format="%Y-%m-%d",exact=False)  
            chunk['time'] = pd.to_datetime(chunk['time'],format="%Y-%m-%d %H:%M:%S")  
            
            # sort and assign a unique index
            chunk.sort(['date','cab_id','time'], inplace=True)
            chunk.index = rowsWritten + pd.Series(range(0,len(chunk)))
            
            # write the data
            store.append(outkey, chunk, data_columns=True)

            rowsWritten += len(chunk)
            print 'Read %i rows and kept %i rows.' % (rowsRead, rowsWritten)
            
        # close the writer
        store.close()
    
    
    def identifyGPSTrips(self, storefile, inkey, outkey):
        """
        Reads the GPS points and creates a sequence of points for each
        taxi trip.  
        
        storefile - HDF datastore with GPS points in it. 
        """

        # open the data store
        store = pd.HDFStore(storefile)    
        
        # get the list of dates and cab_ids to process
        dates = store.select_column(inkey, 'date').unique()
        dates.sort()
        cab_ids = store.select_column(inkey, 'cab_id').unique()
        cab_ids.sort()
        
        # for testing only
        #cab_ids = cab_ids[:5]

        print 'Retrieved a total of %i days to process' % len(dates)
        
        # loop through the dates and cab_ids
        for date in dates: 
            print 'Processing ', date            
            for cab_id in cab_ids:
                print 'Processing cab_id ', cab_id
                    
                # get the data and sort
                query = 'date==Timestamp(date) & cab_id==' + str(cab_id)
                df = store.select('points', where=query)                          
                df.sort(['time'], inplace=True)
                                    
                if (len(df)>0):
                        
                    # initialize the columns  
                    df['feet']  = 0
                    df['seconds'] = 0
                    df['speed']   = 0
                    df['forward_stationary_time'] = 0
                    df['backward_stationary_time'] = 0
                                    
                    # sort out whether the vehicle is moving, and how to group trips
                    first_row = True
                    last_row = None
                    for i, row in df.iterrows():
        
                        # reset for each new vehicle
                        if (first_row):
                            stationary_time = 0
                            first_row = False
                            
                        # for these, calculate measures and increment trip_ids
                        else:
                            pos1 = Position(last_row['x'],last_row['y'])
                            pos2 = Position(row['x'], row['y'])
                            feet = HwyNetwork.distanceInFeet(pos1, pos2)

                            seconds = (row['time'] - last_row['time']).total_seconds()
                            speed = (feet / seconds) * 0.681818
                                
                            # keep track of how long the vehicle is stationary for
                            if (speed < self.SPEED_THRESHOLD):
                                stationary_time += seconds
                            else: 
                                stationary_time = 0    
                                                   
                            df.at[i,'feet']    = feet
                            df.at[i,'seconds'] = seconds
                            df.at[i,'speed']   = speed
                            df.at[i,'forward_stationary_time'] = stationary_time  
                        
                        last_row = row
                        
                    # make a backwards pass to clean up the first and last
                    # GPS point of the trip, which can be stationary       
                    df.sort(['time'], ascending=[0], inplace=True) 
                    first_row = True
                    last_row = 'none'
                    for i, row in df.iterrows():
                            
                        # reset for each new vehicle
                        if (first_row):
                            stationary_time = 0
                            first_row = False
                            
                        # for these, calculate measures and increment trip_ids
                        else:
                            seconds = (last_row['time'] - row['time']).total_seconds()
                                
                            # keep track of how long the vehicle is stationary for
                            if (last_row['speed'] < self.SPEED_THRESHOLD):
                                stationary_time += seconds
                            else: 
                                stationary_time = 0    
                            
                            df.at[i,'backward_stationary_time'] = stationary_time           
                            
                        last_row = row
                    
                    # now make another forward pass to group into trips                  
                    df.sort(['time'], inplace=True) 
                    first_row = True
                    last_row = 'none'
                    df['trip_id'] = -1
                    
                    for i, row in df.iterrows():
                                            
                        # reset for each new vehicle
                        if (first_row):
                            trip_id = 1
                            first_row = False           
        
                        # reset if you change between empty and metered
                        elif (row['status'] != last_row['status']):
                            trip_id += 1
                            
                        # reset if there is a stop
                        elif (row['forward_stationary_time'] > self.TIME_THRESHOLD):
                            trip_id += 1
                            
                        # reset if it is the last point before a stop    
                        elif (row['backward_stationary_time'] > self.TIME_THRESHOLD 
                            and row['forward_stationary_time'] > 0):
                            trip_id += 1
                            
                        # otherwise it is a continuation
        
                        # assign value
                        df.at[i,'trip_id'] = trip_id
                        
                        last_row = row
                                        
                    # determine the points per trip, and distance travelled
                    grouped = df.groupby(['trip_id'])
                    df = grouped.apply(setNumPointsAndLength)
                                                                                                                
                    # filter out the records that don't make valid trips
                    df_filtered = df[(df['num_points']>1.0) & 
                        (df['trip_length'] > self.TRIP_DIST_THRESHOLD)]
                                                                                            
                    # write the data
                    store.append(outkey, df_filtered, data_columns=True)

        # all done
        store.close()


    def createTrajectories(self, hwynet, storefile, inkey, outkey):
        """
        Takes the sequence of points, and converts each into a
        trajectory object. 
        
        storefile - HDF datastore with GPS points in it. 
        """
        
        # open the data store
        store = pd.HDFStore(storefile)    
        
        # get the list of dates and cab_ids to process
        dates = store.select_column(inkey, 'date').unique()
        dates.sort()

        print 'Retrieved a total of %i days to process' % len(dates)
        
        # loop through the dates 
        for date in dates: 
            print 'Processing ', date
            
            # get the data and sort
            gps_df = store.select(inkey, where='date==Timestamp(date)')  
            
            # loop through each trip
            groups = gps_df.groupby(['cab_id','trip_id'])     
            for group in groups:
                print 'Processing group ', group[0]
                
                # group[0] is the index, group[1] is the records
                traj = Trajectory(hwynet, group[1])
                traj.calculateMostLikely()
                
                # allocate trajectory travel times to links
                (link_ids, startTimes, travelTimes) = \
                    self.allocateTrajectoryTravelTimeToLinks(hwynet, traj)
 
                # create a dataframe                 
                data = {'cab_id': group[1].cab_id, 
                        'trip_id': group[1].trip_id, 
                        'status': group[1].status, 
                        'link_id': link_ids, 
                        'start_time': startTimes, 
                        'travel_time': travelTimes}
                link_df = pd.DataFrame(data)
                
                # write the data
                store.append(outkey, link_df, data_columns=True)
        
        # all done
        store.close()

        
    def allocateTrajectoryTravelTimeToLinks(self, hwynet, traj):
        """
        Takes a trajectory, with the most likely already calculated, 
        and allocates the travel time to the links traversed.
        
        Returns three lists for: 
            (link_ids, startTimes, travelTimes)
            There is one record for each link in the trajectory. 
        """
        
        # the output containers
        link_ids    = []
        startTimes  = []
        travelTimes = []
        
        
        # now go through each path
        paths = traj.getMostLikelyPaths()
        path_times = traj.getPathStartEndTimes()
        
        numLinks = 0
        currentTime = None
        for i in range(0, len(paths)):
            (pathStartTime, pathEndTime) = path_times[i]
            pathLinkIds = paths[i].links
            
            # for the first and last link, these are only for the portion
            # of the link that is actually traversed
            pathLinkTravelTimes = hwynet.allocatePathTravelTimeToLinks(
                paths[i], pathStartTime, pathEndTime)
                
            # if it is the very first link, account for the link 
            # start time being a bit earlier than the path start time
            if i==0:
                link_ids.append(pathLinkIds[0])
                                
                # TODO: extra time should be a datetime object or some such thing!
                offsetRatio = hwynet.getLinkOffsetRatio(paths[i].start)
                extraTime = pathLinkTravelTimes[0] * offsetRatio
                linkStartTime = pathStartTime - extraTime
                startTimes.append(linkStartTime)
                
                travelTimes.append(pathLinkTravelTimes[0])
                
                numLinks += 1
                currentTime = linkStartTime + datetime.timedelta(seconds=pathLinkTravelTimes[0])
                
            # if it is the first link in the path, but not the first in 
            # the trajectory, there will be overlap with the previous link
            else:
                travelTime1 = travelTimes[numLinks-1]
                travelTime2 = pathLinkTravelTimes[0]
                offsetRatio = hwynet.getLinkOffsetRatio(paths[i].start)
                travelTime = (travelTime1 * offsetRatio) + (travelTime2 * (1-offsetRatio))
                travelTimes[numLinks-1] = travelTime
                
                currentTime = startTimes[numLinks-1] + datetime.timedelta(seconds=travelTime)
                
            # for subsequent links, it is a bit more straight-forward
            for j in range(1, pathLinkIds):
                link_ids.append(pathLinkIds[j])
                startTimes.append(currentTime)
                travelTimes.append(pathLinkTravelTimes[j])
                
                numLinks += 1
                currentTime = linkStartTime + datetime.timedelta(seconds=pathLinkTravelTimes[j])
                
            return (link_ids, startTimes, travelTimes)
