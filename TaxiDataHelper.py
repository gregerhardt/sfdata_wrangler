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
import datetime
import HwyNetwork
from Trajectory import Trajectory
from mm.path_inference.structures import Position


def setNumPointsAndLength(df):
    """ 
    calculates number of points in each trip
    """
    df['num_points'] = len(df)
    df['trip_length'] = df['feet'].sum()
    return df
                        

def getHour(time): 
    """
    returns the hour given a datetime
    """
    return time.hour                                    


def percentile95(series): 
    """
    returns the 95th percentile of a column
    """
    return np.percentile(series, 95)


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
    TIME_THRESHOLD = 180.0   # seconds

    # if no GPS recordsings are made within this time, it means the unit
    # is not functioning properly, and we should split the trip here
    TIME_BETWEEN_POINTS_THRESHOLD = 300.0  # seconds
    
    # if the distance between GPS points is greater than this, it is 
    # probably a faulty reading, so move to a new trip. 
    DIST_BETWEEN_POINTS_THRESHOLD = 7500.0 # feet
    
    # threshold at which an entire trip (sequence of points) is counted
    #   500 ft is about the length of a city block
    TRIP_DIST_THRESHOLD = 500
    
    # for debug output
    debugFile = None
    debugCabTripIds = None
    

    def __init__(self):
        """
        Constructor.             
        """   
    
    def openDebugFile(self, debugFile):
        """
        Sets the file where we write the debug output. 
        """          
        self.debugFile = open(debugFile, 'a')      
        
    def closeDebugFile(self):
        """
        Closes the debug file. 
        """          
        self.debugFile.close()        
        
        
    def setDebugCabTripIds(self, cabTripIdSet):
        """
        Specifies the set of cab_ids to debug
        """
        self.debugCabTripIds = cabTripIdSet
                                                                
    def processRawData(self, infile, outfile, outkey):
        """
        Read taxi data, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in raw CSV format
        outfile - output file name in h5 format
        """
        
        print (datetime.datetime.now(), 'Converting raw data in file: ', infile)
        
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
            lon_lat = pd.Series(list(zip(chunk['longitude'], chunk['latitude'])), index=chunk.index)
            x_y = lon_lat.apply(HwyNetwork.convertLongitudeLatitudeToXY)
            chunk['x'], chunk['y'] = zip(*x_y)
            
            # keep only the points within the city bounds
            chunk['in_sf'] = x_y.apply(HwyNetwork.isInSanFranciscoBox)
            chunk = chunk[chunk['in_sf']==True]
        
            # convert to timedate formats
            chunk['date_string'] = chunk['time'].str.split(' ').str[0]
            chunk['date'] = pd.to_datetime(chunk['date_string'],format="%Y-%m-%d",exact=False)
            chunk['time'] = pd.to_datetime(chunk['time'],format="%Y-%m-%d %H:%M:%S")  
            
            # sort and assign a unique index
            chunk.sort_values(['date','cab_id','time'], inplace=True)
            chunk.index = rowsWritten + pd.Series(range(0,len(chunk)))
            
            # write the data
            store.append(outkey, chunk, data_columns=True)

            rowsWritten += len(chunk)
            print ('Read %i rows and kept %i rows.' % (rowsRead, rowsWritten))
            
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
        print (dates)
        cab_ids = store.select_column(inkey, 'cab_id').unique()
        cab_ids.sort()
        
        # for testing only
        #cab_ids = cab_ids[:5]

        print ('Retrieved a total of %i days to process' % len(dates))
        
        # loop through the dates and cab_ids
        for date in dates: 
            print ('Processing ', date)            
            for cab_id in cab_ids:
                print ('Processing cab_id ', cab_id)
                    
                # get the data and sort
                query = 'date==Timestamp(date) & cab_id==' + str(cab_id)
                df = store.select('points', where=query)                          
                df.sort_values(['time'], inplace=True)
                                    
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
                    df.sort_values(['time'], ascending=[0], inplace=True) 
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
                    df.sort_values(['time'], inplace=True) 
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
                        
                        # reset if the recordings are not frequent enough
                        elif (row['seconds'] > self.TIME_BETWEEN_POINTS_THRESHOLD):
                            trip_id += 1                            
                            
                        # reset if the distance is too great
                        elif (row['feet'] > self.DIST_BETWEEN_POINTS_THRESHOLD):
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

        print ('Retrieved a total of %i days to process' % len(dates))
        
        # loop through the dates 
        rowsWritten = 0
        for date in dates: 
            print ('Processing ', date)
            
            # get the data and sort
            gps_df = store.select(inkey, where='date==Timestamp(date)')  
            
            # loop through each trip
            last_cab_id = 0
            groups = gps_df.groupby(['cab_id','trip_id','status'])     
            for group in groups:                
                (cab_id, trip_id, status) = group[0]
                if (cab_id != last_cab_id):
                    print ('    Processing cab_id: ', cab_id)
                
                # group[0] is the index, group[1] is the records
                traj = Trajectory(hwynet, group[1])
                
                # check for empty set
                if (len(traj.candidatePoints)==0):
                    continue                
                
                # determine most likely paths and points
                traj.calculateMostLikely()
                
                # for debugging
                if (cab_id, trip_id) in self.debugCabTripIds:
                    traj.printDebugInfo(self.debugFile, ids=(cab_id, trip_id))
                
                # allocate trajectory travel times to links
                (link_ids, traversalRatios, startTimes, travelTimes) = \
                        self.allocateTrajectoryTravelTimeToLinks(hwynet, traj)
 
                # create a dataframe                 
                data = {'link_id': link_ids, 
                        'traversal_ratio': traversalRatios, 
                        'start_time': startTimes, 
                        'travel_time': travelTimes}
                link_df = pd.DataFrame(data)
                
                link_df['date'] = date
                link_df['cab_id']  = cab_id
                link_df['trip_id'] = trip_id
                link_df['status']  = status
                
                last_cab_id = cab_id
                
                # set the index
                link_df.index = rowsWritten + pd.Series(range(0,len(link_df)))
                rowsWritten += len(link_df)
                
                # write the data
                store.append(outkey, link_df, data_columns=True)
        
        # all done
        store.close()

    
    def allocateTrajectoryTravelTimeToLinks(self, hwynet, traj):
        """
        Takes a trajectory, with the most likely already calculated, 
        and allocates the travel time to the links traversed.
        
        Returns three lists for: 
            (link_ids, traversalRatios, startTimes, travelTimes)
            There is one record for each link in the trajectory. 
        """
        
        paths = traj.getMostLikelyPaths()
        path_times = traj.getPathStartEndTimes()
        
        if (len(paths)==0 or paths==[None]):
            return ([],[],[],[])
                        
        # STEP 1: get every link, allowing for duplicates
        link_ids1        = []
        traversalRatios1 = []
        travelTimes1     = []

        for (path, path_time) in zip(paths, path_times):
        
            if (path==None 
                or len(path.links)==0
                or path.start==path.end):
                continue                
            
            (pathStartTime, pathEndTime) = path_time
                    
            (link_id, traversalRatio, travelTime) = \
                hwynet.allocatePathTravelTimeToLinks(
                path, pathStartTime, pathEndTime)
            
            link_ids1 += link_id 
            traversalRatios1 += traversalRatio
            travelTimes1 += travelTime
                        
        # STEP 2: now merge the duplicates
        link_ids2        = []
        traversalRatios2 = []
        travelTimes2     = []
        
        numLinks = 0
        prev_link_id = -999
        totalTraversalRatio = 0.0
        totalTravelTime = 0.0
        
        for (link_id, traversalRatio, travelTime) in \
            zip(link_ids1, traversalRatios1, travelTimes1): 
                        
            # if we've moved to a new link, append the previous value
            # and reset the buckets
            if (link_id!=prev_link_id):                 
                if (numLinks>0): 
                    link_ids2.append(prev_link_id)
                    traversalRatios2.append(totalTraversalRatio)
                    travelTimes2.append(totalTravelTime)
                
                numLinks += 1
                totalTraversalRatio = 0
                totalTravelTime = 0
                
            # in all cases, we sum up the current values
            totalTraversalRatio += traversalRatio
            totalTravelTime += travelTime
            prev_link_id = link_id
            
        # don't forget to append the very last value
        link_ids2.append(prev_link_id)
        traversalRatios2.append(totalTraversalRatio)
        travelTimes2.append(totalTravelTime)
        

        # STEP 3: determine the start times
        startTimes = []
        (firstPathStartTime, firstPathEndTime) = path_times[0]
        currentTime = firstPathStartTime

        for tt in travelTimes2: 
            startTimes.append(currentTime)
            currentTime = currentTime + datetime.timedelta(seconds=tt)
        
        return (link_ids2, traversalRatios2, startTimes, travelTimes2)
    

    def aggregateLinkTravelTimes(self, storefile, inkey, outkey):
        """
        Given individual observations, aggregates link travel times
        to mean values. 
        
        storefile - HDF datastore with trajectories in it. 
        inkey - input key containing trajectories
        outkey - output key containing average link travel times. 
        """

        # open the data store
        store = pd.HDFStore(storefile)    
        
        # get the list of dates and cab_ids to process
        dates = store.select_column(inkey, 'date').unique()
        dates.sort()

        print ('Retrieved a total of %i days to process' % len(dates))
        
        # loop through the dates and cab_ids
        for date in dates: 
            print ('Processing ', date)            
                    
            # get the data--only include cases where we traverse most of the link
            df = store.select(inkey, where='date==Timestamp(date) and traversal_ratio>0.75')  
            
            # some derived fields
            df['hour'] = df['start_time'].apply(getHour)
            df['travel_time'] = df['travel_time'].div(df['traversal_ratio'])

            # group
            aggMethod = {'travel_time' : 
                        {'observations':'count', 
                        'tt_mean':np.mean, 
                        'tt_std':np.std, 
                        'tt_95':percentile95}}

            grouped = df.groupby(['link_id', 'hour'])
            aggregated = grouped.aggregate(aggMethod)
                
            # drop multi-level columns
            levels = aggregated.columns.levels
            labels = aggregated.columns.labels
            aggregated.columns = levels[1][labels[1]]
                                                
            # clean up structure of dataframe
            aggregated = aggregated.sort_index()
            aggregated = aggregated.reset_index()     

            # TODO: switch this to month, dow
            aggregated['date'] = date
            
            # write the data
            store.append(outkey, aggregated, data_columns=True)
            
            #convert the taxi.h5 file to a text file
            import h5py
            np.savetxt('taxi.txt',h5py.File('taxi.h5'),'%g',' ')

