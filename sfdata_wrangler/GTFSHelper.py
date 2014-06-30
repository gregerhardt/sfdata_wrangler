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

import transitfeed  
import pyproj  
from shapely.geometry import Point, LineString  
            

#  define projection from LON, LAT to UTM zone 10
toUTM = pyproj.Proj(proj='utm', zone=10, datum='WGS84', ellps='WGS84', units='m')            
    
                    
def getWrapAroundTime(dateString, timeString):
    """
    Converts a string in the format '%H:%M:%S' to a datetime object.
    Accounts for the convention where service after midnight is counted
    with the previous day, so input times can be >24 hours. 
    """        
    nextDay = False
    hr, min, sec = timeString.split(':')
    if int(hr)>= 24:
        hr = str(int(hr) - 24)
        timeString = hr + ':' + min + ':' + sec
        nextDay = True
        
    datetimeString = dateString + ' ' + timeString    
    time = pd.to_datetime(datetimeString, format='%Y-%m-%d %H:%M:%S')
        
    if nextDay: 
       time = time + pd.DateOffset(days=1)
    
    return time
        

def calculateHeadways(df):
    """
    Calculates the headways for a group. Assumes data are grouped by: 
    ['AGENCY_ID','ROUTE_SHORT_NAME','ROUTE_LONG_NAME','DIR','SEQ']
    (but not by TRIP).     
    """        
    df.sort(['DEPARTURE_TIME_S'], inplace=True)

    lastDeparture = 0
    for i, row in df.iterrows():    
        if lastDeparture==0: 
            df['HEADWAY'][i] = np.NaN        # missing headway for first trip
        else:
            diff = df['DEPARTURE_TIME_S'][i] - lastDeparture
            df['HEADWAY'][i] = round(diff.seconds / 60.0, 2)
        lastDeparture = df['DEPARTURE_TIME_S'][i]
    
    return df                                                
    

def reproject(latitude, longitude):
    """Returns the x & y coordinates in meters using a sinusoidal projection"""
    from math import pi, cos, radians
    earth_radius = 6371009 # in meters
    lat_dist = pi * earth_radius / 180.0

    y = latitude * lat_dist
    x = longitude * lat_dist * cos(radians(latitude))
    return x, y
    
    
class GTFSHelper():
    """ 
    Methods used for loading and converting General Transit Feed Specification
    (GTFS) files, and relating them to AVL/APC data. 
    
    """

    # specifies how to read in each column from raw input files
    #  columnName,       stringLength, index(0/1), source('gtfs', 'avl', 'join' or 'calculated')
    COLUMNS = [
	['MONTH',             0, 0, 'gtfs'],        # Calendar attributes
	['DATE',              0, 1, 'gtfs'],  
        ['DOW',               0, 1, 'gtfs'], 
        ['TOD',               0, 0, 'gtfs'],
        ['AGENCY_ID',        10, 0, 'join'],        # for matching to AVL data
        ['ROUTE_SHORT_NAME', 32, 1, 'join'], 
        ['ROUTE_LONG_NAME',  32, 1, 'join'], 
        ['DIR',               0, 1, 'join'], 
        ['TRIP',              0, 1, 'join'], 
        ['SEQ',               0, 1, 'join'], 
        ['OBSERVED',          0, 1, 'avl'],         # observed in AVL data?
        ['ROUTE_TYPE',        0, 0, 'gtfs'],        # route/trip attributes 
        ['TRIP_HEADSIGN',    32, 0, 'gtfs'], 
	['HEADWAY'   ,        0, 0, 'gtfs'], 
        ['FARE',              0, 0, 'gtfs'], 
	['PATTCODE'  ,       10, 0, 'avl'], 
	['SCHOOL'    ,        0, 0, 'avl'], 
        ['STOPNAME',         32, 0, 'gtfs'],        # stop attributes
	['STOPNAME_AVL',     32, 0, 'avl' ], 
        ['STOP_LAT',          0, 0, 'gtfs'], 
        ['STOP_LON',          0, 0, 'gtfs'], 
        ['SOL',               0, 0, 'gtfs'],
        ['EOL',               0, 0, 'gtfs'],
	['TIMEPOINT' ,        0, 0, 'avl' ], 
        ['ARRIVAL_TIME_S',    0, 0, 'gtfs'],        # times
	['ARRIVAL_TIME'  ,    0, 0, 'avl'], 
	['ARRIVAL_TIME_DEV',  0, 0, 'calculated'], 
        ['DEPARTURE_TIME_S',  0, 0, 'gtfs'], 
	['DEPARTURE_TIME' ,   0, 0, 'avl'], 
	['DEPARTURE_TIME_DEV',0, 0, 'calculated'], 
	['DWELL_S'   ,        0, 0, 'gtfs'], 
	['DWELL'     ,        0, 0, 'avl'], 
	['RUNTIME_S' ,        0, 0, 'gtfs'], 
	['RUNTIME'   ,        0, 0, 'avl'], 
	['SERVMILES' ,        0, 0, 'gtfs'], 
	['SERVMILES_AVL',     0, 0, 'avl'],         # Distances and speeds
	['RUNSPEED_S' ,       0, 0, 'gtfs'], 
	['RUNSPEED'   ,       0, 0, 'calculated'], 
	['ONTIME2'   ,        0, 0, 'calculated'], 
	['ONTIME10'  ,        0, 0, 'calculated'], 
	['ON'        ,        0, 0, 'avl'], # ridership
	['OFF'       ,        0, 0, 'avl'], 
	['LOAD_ARR'  ,        0, 0, 'avl'], 
	['LOAD_DEP'  ,        0, 0, 'avl'], 
	['PASSMILES' ,        0, 0, 'calculated'], 
	['PASSHOURS',         0, 0, 'calculated'], 
	['WAITHOURS',         0, 0, 'calculated'], 
	['PASSDELAY_DEP',     0, 0, 'calculated'], 
	['PASSDELAY_ARR',     0, 0, 'calculated'], 
	['RDBRDNGS'  ,        0, 0, 'avl'], 
	['CAPACITY'  ,        0, 0, 'avl'], 
	['DOORCYCLES',        0, 0, 'avl'], 
	['WHEELCHAIR',        0, 0, 'avl'], 
	['BIKERACK'  ,        0, 0, 'avl'], 	
        ['ROUTE_ID',          0, 0, 'gtfs'],  # additional IDs 
        ['ROUTE_AVL',         0, 0, 'avl'],   
        ['TRIP_ID',           0, 0, 'gtfs'], 
        ['STOP_ID',           0, 0, 'gtfs'], 
	['STOP_AVL'  ,        0, 0, 'avl'], 
        ['BLOCK_ID',          0, 0, 'gtfs'], 
        ['SHAPE_ID',          0, 0, 'gtfs'],
        ['SHAPE_DIST',        0, 0, 'gtfs'],
	['VEHNO'     ,        0, 0, 'avl'], 
        ['SCHED_START',       0, 0, 'gtfs'],  # range of this GTFS schedule
        ['SCHED_END',         0, 0, 'gtfs']
        ]
                
   

    def processRawData(self, infile, outfile):
        """
        Read GTFS, cleans it, processes it, and writes it to an HDF5 file.
        This will be done for every individual day, so you get a list of 
        every bus that runs. 
        
        infile  - in GTFS format
        outfile - output file name in h5 format, same as AVL/APC format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # convert column specs 
        colnames = []   
        stringLengths= {}
        indexColumns = []
        for col in self.COLUMNS: 
            name = col[0]
            stringLength = col[1]
            index = col[2]
            source = col[3]
            
            if source=='gtfs' or source=='join': 
                colnames.append(name)
                if (stringLength>0): 
                    stringLengths[name] = stringLength
                if index==1: 
                    indexColumns.append(name)
                        
        # establish the feed
        tfl = transitfeed.Loader(feed_path=infile)
        schedule = tfl.Load()
        
        # determine the dates
        dateRange = schedule.GetDateRange()
        startDate = pd.to_datetime(dateRange[0], format='%Y%m%d')
        endDate   = pd.to_datetime(dateRange[1], format='%Y%m%d')

        # create dictionary with one dataframe for each service period
        dataframes = {}
        servicePeriods = schedule.GetServicePeriodList()
        for period in servicePeriods:
            
            # create an empty list of dictionaries to store the data
            data = []
        
            # create one record for each trip-stop, specific to the service
            # on this day
            tripList = schedule.GetTripList()
            for trip in tripList:
                if trip.service_id == period.service_id:
                        
                    # determine route attributes
                    route = schedule.GetRoute(trip.route_id)
                    
                    # get shape attributes, converted to a line
                    shape = schedule.GetShape(trip.shape_id)
                    shapePoints = []
                    for p in shape.points: 
                        x, y = toUTM(p[1], p[0])
                        shapePoints.append((x, y))
                    shapeLine = LineString(shapePoints)
                    
                    
                    # calculate fare--assume just based on route ID
                    fare = 0
                    fareAttributeList = schedule.GetFareAttributeList()
                    for fareAttribute in fareAttributeList:
                        fareRuleList = fareAttribute.GetFareRuleList()
                        for fareRule in fareRuleList:
                            if fareRule.route_id == trip.route_id: 
                                fare = fareAttribute.price
                        
                    # one record for each stop time
                    stopTimeList = trip.GetStopTimes()    
                    
                    # initialize for looping
                    i = 0        
                    lastDepartureTime = startDate
                    
                    for stopTime in stopTimeList:
                        record = {}
                        
                        # first stop, last stop and trip based on order
                        if i==0: 
                            startOfLine = 1
                            hr, min, sec = stopTime.departure_time.split(':')
                            firstDeparture = int(hr + min)
            
                            # compute TEP time periods -- need to iterate
                            if (firstDeparture >= 300  and firstDeparture < 600):  
                                timeOfDay='0300-0559'
                            elif (firstDeparture >= 600  and firstDeparture < 900):  
                                timeOfDay='0600-0859'
                            elif (firstDeparture >= 900  and firstDeparture < 1400): 
                                timeOfDay='0900-1359'
                            elif (firstDeparture >= 1400 and firstDeparture < 1600): 
                                timeOfDay='1400-1559'
                            elif (firstDeparture >= 1600 and firstDeparture < 1900): 
                                timeOfDay='1600-1859'
                            elif (firstDeparture >= 1900 and firstDeparture < 2200): 
                                timeOfDay='1900-2159'
                            elif (firstDeparture >= 2200 and firstDeparture < 9999): 
                                timeOfDay='2200-0259'
                            else:
                                timeOfDay=''
                                
                            # distance traveled along shape for previous stop
                            lastDistanceTraveled = 0
                        else:
                            startOfLine = 0
                            
                        if i==(len(stopTimeList)-1):
                            endOfLine = 1
                        else: 
                            endOfLine = 0
                        
                        # calendar attributes
                        record['MONTH'] = startDate
                        record['DATE'] = startDate
                        record['DOW']  = int(trip.service_id)
                        record['TOD']  = timeOfDay
        
                        # For matching to AVL data
                        record['AGENCY_ID']        = str(route.agency_id)
                        record['ROUTE_SHORT_NAME'] = str(route.route_short_name)
                        record['ROUTE_LONG_NAME']  = str(route.route_long_name)
                        record['DIR']              = int(trip.direction_id)
                        record['TRIP']             = firstDeparture    # contains HHMM of departure from first stop
                        record['SEQ']              = int(stopTime.stop_sequence)                            
                            
                        # route/trip attributes
                        record['ROUTE_TYPE']       = int(route.route_type)
                        record['TRIP_HEADSIGN']    = str(trip.trip_headsign)
                        record['HEADWAY']          = np.NaN             # calculated below
                        record['FARE']             = float(fare)  
                        
                        # stop attriutes
                        record['STOPNAME']         = str(stopTime.stop.stop_name)
                        record['STOP_LAT']         = float(stopTime.stop.stop_lat)
                        record['STOP_LON']         = float(stopTime.stop.stop_lon)
                        record['SOL']              = startOfLine
                        record['EOL']              = endOfLine
                        
                        # stop times        
                        # deal with wrap-around aspect of time (past midnight >2400)
                        arrivalTime = getWrapAroundTime(str(startDate.date()), stopTime.arrival_time)
                        departureTime = getWrapAroundTime(str(startDate.date()), stopTime.departure_time)
                        if startOfLine or endOfLine: 
                            dwellTime = 0
                        else: 
                            timeDiff = departureTime - arrivalTime
                            dwellTime = round(timeDiff.seconds / 60.0, 2)

                        record['ARRIVAL_TIME_S']   = arrivalTime
                        record['DEPARTURE_TIME_S'] = departureTime
                        record['DWELL_S']          = dwellTime
                        
                        # runtimes
                        if startOfLine: 
                            runtime = 0
                        else: 
                            timeDiff = arrivalTime - lastDepartureTime
                            runtime = round(timeDiff.seconds / 60.0, 2)
                        record['RUNTIME_S'] = runtime
                        
                        # location along shape object (SFMTA uses meters)
                        if stopTime.shape_dist_traveled > 0: 
                            record['SHAPE_DIST'] = stopTime.shape_dist_traveled
                        else: 
                            x, y = toUTM(stopTime.stop.stop_lon, stopTime.stop.stop_lat)
                            stopPoint = Point(x, y)
                            projectedDist = shapeLine.project(stopPoint, normalized=True)
                            distanceTraveled = shape.max_distance * projectedDist
                            record['SHAPE_DIST'] = distanceTraveled

                        # service miles
                        if startOfLine: 
                            serviceMiles = 0
                        else: 
                            serviceMiles = round((distanceTraveled - lastDistanceTraveled) / 1609.344, 3)
                        record['SERVMILES'] = serviceMiles
                            
                        # speed (mph)
                        if runtime > 0: 
                            record['RUNSPEED_S'] = round(serviceMiles / (runtime / 60.0), 2)
                        else:
                            record['RUNSPEED_S'] = 0
                                                  

                        # Additional GTFS IDs.        
                        record['ROUTE_ID']       = int(trip.route_id)
                        record['TRIP_ID']        = int(trip.trip_id)
                        record['STOP_ID']        = int(stopTime.stop.stop_id)
                        record['BLOCK_ID']       = int(trip.block_id)
                        record['SHAPE_ID']       = int(trip.shape_id)
                        
                        # indicates range this schedule is in operation    
                        record['SCHED_START']    = startDate            # start date for this schedule
                        record['SCHED_END']      = endDate              # end date for this schedule    
                        
                        # track from previous record
                        lastDepartureTime = departureTime      
                        lastDistanceTraveled = distanceTraveled                              
                                                                                                                                    
                        data.append(record)                
                        i += 1
                                    
            # convert to data frame
            print "service_id %s has %i trip-stop records" % (period.service_id, len(data))
            df = pd.DataFrame(data)            
            
            # calculate the headways, based on difference in previous bus on 
            # this route stopping at the same stop
            groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','ROUTE_LONG_NAME','DIR','SEQ']
            df = df.groupby(groupby).apply(calculateHeadways)
            df = df.drop(['AGENCY_ID','ROUTE_SHORT_NAME','ROUTE_LONG_NAME','DIR','SEQ'], axis=1)
            df = df.reset_index()
            
            # keep only relevant columns, sorted
            df.sort(indexColumns, inplace=True)                        
            df = df[colnames]
            
            # keep one dataframe for each service period
            dataframes[period.service_id] = df

        # loop through each date, and add the appropriate service to the database
        print 'Writing data for periods from ', startDate, ' to ', endDate
        store = pd.HDFStore(outfile)

        servicePeriodsEachDate = schedule.GetServicePeriodsActiveEachDate(startDate, endDate)        
        for date, servicePeriods in servicePeriodsEachDate:
            
            month = ((pd.to_datetime(date)).to_period('month')).to_timestamp()            
            
            for period in servicePeriods: 
                print 'Writing ', date
        
                df = dataframes[period.service_id]
                
                # update the dates
                for i, row in df.iterrows():
                    df['ARRIVAL_TIME_S'][i] = date + (df['ARRIVAL_TIME_S'][i] - df['DATE'][i])
                    df['DEPARTURE_TIME_S'][i] = date + (df['DEPARTURE_TIME_S'][i] - df['DATE'][i])
                df['DATE'] = date
                df['MONTH'] = month
        
                store.append('gtfs', df, data_columns=True, 
                            min_itemsize=stringLengths)

        store.close()


    def joinSFMuniData(self, gtfs_file, sfmuni_file, joined_outfile):
        """
        Left join from GTFS to SFMuni sample.        
        
        gtfs_file - HDF file containing processed GTFS data      
        sfmuni_file - HDF file containing processed, just for sampled routes
        joined_outfile - HDF file containing merged GTFS and SFMuni data     
        """
        
        # convert column specs 
        colnames = []   
        stringLengths= {}
        indexColumns = []
        joinFields = []
        sources = {}
        for col in self.COLUMNS: 
            name = col[0]
            stringLength = col[1]
            index = col[2]
            source = col[3]
            
            colnames.append(name)
            sources[name] = source
            if (stringLength>0): 
                stringLengths[name] = stringLength
            if index==1: 
                indexColumns.append(name)
            if source=='join': 
                joinFields.append(name)
        
        # establish the stores
        gtfs_store   = pd.HDFStore(gtfs_file)
        sfmuni_store = pd.HDFStore(sfmuni_file)
        out_store    = pd.HDFStore(joined_outfile)
        
        
        # get the list of all dates in data set
        dates = gtfs_store.select_column('gtfs', 'DATE').unique()
        dates.sort()
        print 'Retrieved a total of %i dates to process' % len(dates)

        # loop through the dates, and aggregate each individually
        for date in dates: 
            print 'Processing ', date          

            # get the observed data
            sfmuni = sfmuni_store.select('sample', where='DATE==Timestamp(date)')
            sfmuni['OBSERVED'] = 1
            
            # join to the gtfs data (make sure they are sorted on the join)
            gtfs   = gtfs_store.select('gtfs', where='DATE==Timestamp(date)')
            joined = pd.merge(gtfs, sfmuni, how='left', on=joinFields, 
                                suffixes=('', '_AVL'), sort=True)

            # initialize derived fields as missing
            joined['RUNTIME'] = np.NaN
            joined['RUNSPEED'] = np.NaN
            joined['ARRIVAL_TIME_DEV']   = np.NaN
            joined['DEPARTURE_TIME_DEV'] = np.NaN
            joined['ONTIME2']  = np.NaN
            joined['ONTIME10'] = np.NaN
            joined['PASSMILES']   = np.NaN
            joined['PASSHOURS']   = np.NaN
            joined['WAITHOURS']   = np.NaN
            joined['PASSDELAY_DEP'] = np.NaN
            joined['PASSDELAY_ARR'] = np.NaN

            # calculate derived fields, in overlapping frames           
            lastRoute = 0
            lastDir = 0
            lastTrip = 0
            lastDepartureTime = 0
            for i, row in joined.iterrows():
                if joined['OBSERVED'][i] == 1: 
                    
                    # observed runtime
                    if (joined['ROUTE_AVL'][i]==lastRoute 
                        and joined['DIR'][i]==lastDir 
                        and joined['TRIP'][i]==lastTrip): 

                        diff = joined['ARRIVAL_TIME'][i] - lastDepartureTime
                        runtime = round(diff.seconds / 60.0, 2)
                    else: 
                        runtime = 0
                        
                        lastRoute = joined['ROUTE_AVL'][i]
                        lastDir = joined['DIR'][i]
                        lastTrip = joined['TRIP'][i]
                        
                    joined['RUNTIME'][i] = runtime
                    lastDepartureTime = joined['DEPARTURE_TIME'][i]

                    # observed speed
                    if runtime>0: 
                        joined['RUNSPEED'][i] = round(joined['SERVMILES'][i] / (runtime / 60.0), 2)
                    else: 
                        joined['RUNSPEED'][i] = 0
                    
            
                    # deviation from scheduled arrival
                    if joined['ARRIVAL_TIME'][i] >= joined['ARRIVAL_TIME_S'][i]: 
                        diff = joined['ARRIVAL_TIME'][i] - joined['ARRIVAL_TIME_S'][i]
                        arrivalTimeDeviation = round(diff.seconds / 60.0, 2)
                    else: 
                        diff = joined['ARRIVAL_TIME_S'][i] - joined['ARRIVAL_TIME'][i]
                        arrivalTimeDeviation = - round(diff.seconds / 60.0, 2)                        
                    joined['ARRIVAL_TIME_DEV'][i] = arrivalTimeDeviation
    
                    # deviation from scheduled departure
                    if joined['DEPARTURE_TIME'][i] >= joined['DEPARTURE_TIME_S'][i]: 
                        diff = joined['DEPARTURE_TIME'][i] - joined['DEPARTURE_TIME_S'][i]
                        departureTimeDeviation = round(diff.seconds / 60.0, 2)
                    else: 
                        diff = joined['DEPARTURE_TIME_S'][i] - joined['DEPARTURE_TIME'][i]
                        departureTimeDeviation = - round(diff.seconds / 60.0, 2)                        
                    joined['DEPARTURE_TIME_DEV'][i] = departureTimeDeviation
                    
                    # ontime, within 2 minutes
                    if arrivalTimeDeviation < 2: 
                        joined['ONTIME2'][i] = 1
                    else: 
                        joined['ONTIME2'][i] = 0
                    
                    # ontime, within 10 minutes
                    if arrivalTimeDeviation < 10: 
                        joined['ONTIME10'][i] = 1
                    else: 
                        joined['ONTIME10'][i] = 0
                    
                    # passenger miles traveled
                    joined['PASSMILES'][i] = joined['LOAD_ARR'][i] * joined['SERVMILES'][i]                
                    
                    # passenger hours -- scheduled time
                    joined['PASSHOURS'][i] = (joined['LOAD_ARR'][i] * joined['RUNTIME_S'][i] 
                                            + joined['LOAD_DEP'][i] * joined['DWELL_S'][i]) / 60.0
                                                                                        
                    # passenger hours of waiting time -- scheduled time
                    joined['WAITHOURS'][i] = (joined['ON'][i] 
                                        * 0.5 * joined['HEADWAY'][i]) / 60.0
                    
                    # passenger hours of delay at departure
                    if departureTimeDeviation > 0: 
                        joined['PASSDELAY_DEP'][i] = (joined['ON'][i] 
                                            * departureTimeDeviation) / 60.0
                    else: 
                        joined['PASSDELAY_DEP'][i] = 0                    
                    
                    # passenger hours of delay at arrival  
                    if arrivalTimeDeviation > 0: 
                        joined['PASSDELAY_ARR'][i] = (joined['OFF'][i] 
                                            * arrivalTimeDeviation) / 60.0
                    else: 
                        joined['PASSDELAY_ARR'][i] = 0        
                else: 
                    joined['OBSERVED'] = 0    
                        
            # keep only relevant columns, sorted
            joined.sort(indexColumns, inplace=True)                        
            joined = joined[colnames]
            
            # write the data
            out_store.append('expanded', joined, data_columns=True, 
                            min_itemsize=stringLengths)
            
        # close up shop
        gtfs_store.close()
        sfmuni_store.close()
        out_store.close()

    