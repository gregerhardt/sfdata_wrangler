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
from pyproj import Proj
from shapely.geometry import Point, LineString  

from SFMuniDataAggregator import SFMuniDataAggregator
            
            
def convertLongitudeLatitudeToXY(lon_lat):        
    """
    Converts longitude and latitude to an x,y coordinate pair in
    NAD83 Datum (most of our GIS and CUBE files)
    
    Returns (x,y) in feet.
    """
    FEET_TO_METERS = 0.3048006096012192
    
    (longitude,latitude) = lon_lat

    p = Proj(proj  = 'lcc',
            datum = "NAD83",
            lon_0 = "-120.5",
            lat_1 = "38.43333333333",
            lat_2 = "37.066666666667",
            lat_0 = "36.5",
            ellps = "GRS80",
            units = "m",
            x_0   = 2000000,
            y_0   = 500000) #use kwargs
    x_meters,y_meters = p(longitude,latitude,inverse=False,errcheck=True)

    return (x_meters/FEET_TO_METERS,y_meters/FEET_TO_METERS)
        
    
                    
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
    ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','SEQ']
    (but not by TRIP).     
    """        
    df.sort(['DEPARTURE_TIME_S'], inplace=True)

    lastDeparture = 0
    for i, row in df.iterrows():    
        if lastDeparture==0: 
            df.at[i,'HEADWAY_S'] = np.NaN        # missing headway for first trip
        else:
            diff = row['DEPARTURE_TIME_S'] - lastDeparture
            df.at[i,'HEADWAY_S'] = round(diff.seconds / 60.0, 2)
        lastDeparture = row['DEPARTURE_TIME_S']
    
    return df                                                
    

def calculateRuntime(df):
    """
    Calculates the runtime between trip_stops. Assumes data are grouped by: 
    ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','TRIP']
    """        
    df.sort(['SEQ'], inplace=True)

    firstStop = True
    lastDepartureTime = None
    for i, row in df.iterrows():    
        if firstStop: 
            df.at[i,'RUNTIME'] = 0        # no runtime for first trip
            firstStop = False
        else:
            diff = row['ARRIVAL_TIME'] - lastDepartureTime
            df.at[i,'RUNTIME'] = max(0, round(diff.total_seconds() / 60.0, 2))
        lastDepartureTime = row['DEPARTURE_TIME']
    
    return df
    
    
def updateTripId(df):
    """
    Updates the trip id to include the first SEQ number. 
    Assumes data are grouped by: 
    ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
    """        
    
    df['TRIP'] = df['TRIP'].astype(str) + '_' + str(min(df['SEQ']))
    return df
                    

def updateSpeeds(speedInputs):
    """
    Calculates the speed based on a tuple (servmiles, runtime)
                                       
    """
        
    (servmiles, runtime) = speedInputs
    
    if runtime>0: 
        return round(servmiles / (runtime / 60.0), 2)
    elif runtime == 0: 
        return 0.0
    else: 
        return np.nan
        

def getScheduleDeviation((actualTime, schedTime)):
    """
    Calculates the speed based on a tuple (servmiles, runtime)
                                       
    """
    if pd.isnull(actualTime): 
        return np.nan
    elif (actualTime >= schedTime):
        diff = actualTime - schedTime
        return round(diff.seconds / 60.0, 2)
    else: 
        diff = schedTime - actualTime
        return -round(diff.seconds / 60.0, 2)
        

def reproject(latitude, longitude):
    """Returns the x & y coordinates in meters using a sinusoidal projection"""
    from math import pi, cos, radians
    earth_radius = 6371009 # in meters
    lat_dist = pi * earth_radius / 180.0

    y = latitude * lat_dist
    x = longitude * lat_dist * cos(radians(latitude))
    return x, y


def getOutfile(filename, date):
    """
    gets a filename with the year replacing YYYY
    """
    return filename.replace('YYYY', str(date.year))


def getOutkey(month, dow, prefix):
    """
    gets the key name as a string from the month and the day of week
    """
    return prefix + str(month.date()).replace('-', '') + 'd' + str(dow)
                    

def calcGroupWeights(df, oldWeight):
    """
    df - dataframe to operate on.  Must contain columns for TRIPS
         and for the oldWeight.  
    oldWeight - column name in df containing the previous weight. 
    
    Intended to operate on a group and weight up to the TRIP_STOPS
    in the group.  
    
    """
    
    obs = float((df[oldWeight] * df['TRIPS']).sum())
    tot = float(df['TRIPS'].sum())    
    if obs>0:
        factor = tot / obs
    else: 
        factor = np.nan
        
    out = df[oldWeight] * factor   
    
    return out


def calcWeights(df, groupby, oldWeight):
    """
    df - dataframe to operate on.  Must contain columns for TRIPS
         and for the oldWeight.  
    groupby - list of columns for grouping dataframe
    oldWeight - column name in df containing the previous weight. 
    
    groups the dataframe as specified, and calculates weights to 
    match the total trips in each group. 
    
    returns series with the weights, and same index df    
    """
    
    grouped = df.groupby(groupby)
    
    # special case if only one group
    if (len(grouped)<=1):
        return calcGroupWeights(df, oldWeight)
    else: 
        weights = grouped.apply(calcGroupWeights, oldWeight)   
        weights = weights.reset_index(level=groupby)
        return weights[oldWeight]
    

            
    
class GTFSHelper():
    """ 
    Methods used for loading and converting General Transit Feed Specification
    (GTFS) files, and relating them to AVL/APC data. 
    
    """

    # code corresponding to busses (drop light rail and cable car)
    BUS_ROUTE_TYPE = 3

    # specifies how to read in each column from raw input files
    #  columnName,       stringLength, index(0/1), source('gtfs', 'avl', 'join' or 'calculated')
    COLUMNS = [
	['MONTH',             0, 0, 'gtfs'],        # Calendar attributes
	['DATE',              0, 1, 'gtfs'],  
        ['DOW',               0, 1, 'gtfs'], 
        ['TOD',              10, 1, 'gtfs'],
        ['AGENCY_ID',        10, 0, 'join'],        # for matching to AVL data
        ['ROUTE_SHORT_NAME', 32, 1, 'join'], 
        ['ROUTE_LONG_NAME',  32, 1, 'gtfs'],        # can have case/spelling differences on long name
        ['DIR',               0, 1, 'join'], 
        ['TRIP',              0, 1, 'join'], 
        ['SEQ',               0, 1, 'join'], 
        ['TRIP_STOPS',        0, 0, 'gtfs'],        # total number of trip-stops
        ['OBSERVED',          0, 0, 'gtfs'],        # observed in AVL data?
        ['ROUTE_TYPE',        0, 0, 'gtfs'],        # route/trip attributes 
        ['TRIP_HEADSIGN',    64, 0, 'gtfs'], 
	['HEADWAY_S' ,        0, 0, 'gtfs'], 
        ['FARE',              0, 0, 'gtfs'], 
	['PATTCODE'  ,       10, 0, 'avl'], 
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
	['TOTTIME_S' ,        0, 0, 'gtfs'], 
	['TOTTIME'   ,        0, 0, 'avl'], 
	['SERVMILES_S' ,      0, 0, 'gtfs'], 
	['SERVMILES',         0, 0, 'avl'],         # Distances and speeds
	['RUNSPEED_S' ,       0, 0, 'gtfs'], 
	['RUNSPEED'   ,       0, 0, 'calculated'], 
	['TOTSPEED_S' ,       0, 0, 'gtfs'], 
	['TOTSPEED'   ,       0, 0, 'calculated'], 
	['ONTIME5'   ,        0, 0, 'calculated'], 
	['ON'        ,        0, 0, 'avl'],           # ridership
	['OFF'       ,        0, 0, 'avl'], 
	['LOAD_ARR'  ,        0, 0, 'avl'], 
	['LOAD_DEP'  ,        0, 0, 'avl'], 
	['PASSMILES' ,        0, 0, 'calculated'], 
	['PASSHOURS',         0, 0, 'calculated'], 
	['WAITHOURS',         0, 0, 'calculated'], 
	['FULLFARE_REV',      0, 0, 'calculated'],     # revenue if all passengers paid full fare
	['PASSDELAY_DEP',     0, 0, 'calculated'], 
	['PASSDELAY_ARR',     0, 0, 'calculated'], 
	['RDBRDNGS'  ,        0, 0, 'avl'], 
	['CAPACITY'  ,        0, 0, 'avl'], 
	['DOORCYCLES',        0, 0, 'avl'], 
	['WHEELCHAIR',        0, 0, 'avl'], 
	['BIKERACK'  ,        0, 0, 'avl'], 	
	['VC' ,               0, 0, 'calculated'],   # crowding
	['CROWDED',           0, 0, 'calculated'], 
	['CROWDHOURS',        0, 0, 'calculated'], 
        ['ROUTE_ID',          0, 0, 'gtfs'],  # additional IDs 
        ['ROUTE_AVL',         0, 0, 'avl'],   
        ['TRIP_ID',           0, 0, 'gtfs'], 
        ['STOP_ID',           0, 0, 'gtfs'], 
	['STOP_AVL'  ,        0, 0, 'avl'], 
        ['BLOCK_ID',          0, 0, 'gtfs'], 
        ['SHAPE_ID',          0, 0, 'gtfs'],
        ['SHAPE_DIST',        0, 0, 'gtfs'],
	['VEHNO'     ,        0, 0, 'avl'], 
        ['SCHED_DATES',      20, 0, 'gtfs']  # range of this GTFS schedule
        ]
                
    

    def __init__(self, sfmuni_file, trip_outfile, ts_outfile, 
                 daily_trip_outfile, daily_ts_outfile,
                 dow=[1,2,3], startDate='1900-01-01', endDate='2100-01-01', 
                 startingTripCount=1, startingTsCount=0):
        """
        Constructor.                 
        """        
                
        # set the relevant files
        self.trip_outfile = trip_outfile
        self.ts_outfile = ts_outfile

        # open the data stores
        self.sfmuni_store = pd.HDFStore(sfmuni_file) 
        
        # which days of week to run for
        self.dow = dow
        
        # helper for creating data aggregations
        self.aggregator = SFMuniDataAggregator(daily_trip_outfile=daily_trip_outfile, 
                                               daily_ts_outfile=daily_ts_outfile)
        
        # count the trips and trip-stops to ensure a unique index
        self.tripCount = startingTripCount
        self.tsCount = startingTsCount

        # get the list of all observed dates
        observedDates = self.sfmuni_store.select_column('sample', 'DATE').unique()
        
        self.dateList = []
        for d in sorted(observedDates): 
            date = pd.Timestamp(d)
            if (date>=pd.Timestamp(startDate) and date<=pd.Timestamp(endDate)):
                self.dateList.append(date)
        
        print 'GTFSHelper set up for ', len(self.dateList), ' observed dates between ', \
               self.dateList[0], ' and ', self.dateList[len(self.dateList)-1]
    
    
    def closeStores(self):  
        """
        Closes all datastores. 
        """
        self.sfmuni_store.close()
        self.aggregator.close()
        
                
    def expandAndWeight(self, gtfs_file):
        """
        Read GTFS, cleans it, processes it, and writes it to an HDF5 file.
        This will be done for every individual day, so you get a list of 
        every bus that runs. 
        
        infile  - in GTFS format
        outfile - output file name in h5 format, same as AVL/APC format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', gtfs_file
              
        # establish the feed
        tfl = transitfeed.Loader(feed_path=gtfs_file)
        schedule = tfl.Load()
        
        # create dictionary with one dataframe for each service period
        dataframes = {}
        servicePeriods = schedule.GetServicePeriodList()        
        for period in servicePeriods:   
            if int(period.service_id) in self.dow:         
                dataframes[period.service_id]  = self.getGTFSDataFrame(schedule, period)
           
        
        # loop through each date, and add the appropriate service to the database  
        gtfsDateRange = schedule.GetDateRange()
        gtfsStartDate = pd.to_datetime(gtfsDateRange[0], format='%Y%m%d')
        gtfsEndDate   = pd.to_datetime(gtfsDateRange[1], format='%Y%m%d')
        servicePeriodsEachDate = schedule.GetServicePeriodsActiveEachDate(gtfsStartDate, gtfsEndDate) 
                   
        print 'Writing data for periods from ', gtfsStartDate, ' to ', gtfsEndDate
        for date, servicePeriodsForDate in servicePeriodsEachDate:           
                        
            if pd.Timestamp(date) in self.dateList:           
                print datetime.datetime.now(), ' Processing ', date         
                
                # use a separate file for each year
                # and write a separate table for each month and DOW
                # format of the table name is mYYYYMMDDdX, where X is the day of week
                month = ((pd.to_datetime(date)).to_period('month')).to_timestamp()    
                trip_outstore = pd.HDFStore(getOutfile(self.trip_outfile, month))  
                ts_outstore = pd.HDFStore(getOutfile(self.ts_outfile, month))  
                
                for period in servicePeriodsForDate: 
                    if int(period.service_id) in self.dow:     
                        
                        outkey = getOutkey(month=month, dow=period.service_id, prefix='m')                                             
    
                        # get the corresponding MUNI data for this date
                        sfmuni = self.getSFMuniData(date)
                            
                        # get the corresponding GTFS dataframe
                        df = dataframes[period.service_id]
                                
                        # update the dates
                        df['ARRIVAL_TIME_S']   = date + (df['ARRIVAL_TIME_S'] - df['DATE'])
                        df['DEPARTURE_TIME_S'] = date + (df['DEPARTURE_TIME_S'] - df['DATE'])
        
                        df['DATE'] = date
                        df['MONTH'] = month
                
                        # join the sfmuni data
                        joined = self.joinSFMuniData(df, sfmuni)    
                            
                        # aggregate from trip-stops to trips
                        trips = self.aggregator.aggregateToTrips(joined)
                            
                        # set a unique trip index
                        trips.index = self.tripCount + pd.Series(range(0,len(trips)))
                        self.tripCount += len(trips)
                
                        # weight the trips
                        trips = self.weightTrips(trips)
                            
                        # write the trips   
                        stringLengths = self.getStringLengths(trips.columns)                                                                    
                        trip_outstore.append(outkey, trips, data_columns=True, 
                                    min_itemsize=stringLengths)
                            
                        # add weights to trip-stop df                          
                        mergeFields = ['DATE','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'TRIP']
                        weightFields = ['PATTERN', 'TRIP_WEIGHT', 'TOD_WEIGHT', 'DAY_WEIGHT', 'SYSTEM_WEIGHT'] 
                        tripWeights = trips[mergeFields + weightFields]            
                        ts = pd.merge(joined, tripWeights, how='left', on=mergeFields, sort=True)  
                            
                        # set a unique trip-stop index
                        ts.index = self.tsCount + pd.Series(range(0,len(ts)))
                        self.tsCount += len(ts)
                            
                        # write the trip-stops        
                        stringLengths = self.getStringLengths(ts.columns)                                                     
                        ts_outstore.append(outkey, ts, data_columns=True, 
                                    min_itemsize=stringLengths)

                        # aggregate to TOD and daily totals, and write those
                        self.aggregator.aggregateTripsToDays(trips)
                        self.aggregator.aggregateTripStopsToDays(ts)
                        
                                                
                trip_outstore.close()
                ts_outstore.close()


    def getSFMuniData(self, date):
        """
        Returns a dataframe with the observed SFMuni records
        and some processing of those
        """

        sfmuni = self.sfmuni_store.select('sample', where='DATE==Timestamp(date)')
        
        # update the TRIP id in case there are multiple trips with different 
        # patterns leaving a different stop at the same time
        groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        sfmuni = sfmuni.groupby(groupby, as_index=False).apply(updateTripId)     
         
                                        
        # calculate observed RUNTIME
        # happens here because the values in the AVL data look screwy.
        groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','TRIP']
        sfmuni = sfmuni.groupby(groupby, as_index=False).apply(calculateRuntime)
        sfmuni['TOTTIME'] = sfmuni['RUNTIME'] + sfmuni['DWELL']                            
                            
                            
        # speed   
        speedInput = pd.Series(zip(sfmuni['SERVMILES'],  sfmuni['RUNTIME']), index=sfmuni.index)     
        sfmuni['RUNSPEED'] = speedInput.apply(updateSpeeds)                    
        speedInput = pd.Series(zip(sfmuni['SERVMILES'], sfmuni['TOTTIME']), index=sfmuni.index)     
        sfmuni['TOTSPEED'] = speedInput.apply(updateSpeeds)
        
        return sfmuni
                    
    
    def getGTFSDataFrame(self, schedule, period):
        """
        Converts the schedule into a dataframe for the given period
        """
        
        # convert column specs 
        colnames = []   
        indexColumns = []
        for col in self.COLUMNS: 
            name = col[0]
            index = col[2]
            source = col[3]
            
            if source=='gtfs' or source=='join': 
                colnames.append(name)
            if index==1: 
                indexColumns.append(name)
                
        # create an empty list of dictionaries to store the data
        data = []
        
        # determine the dates
        dateRange = schedule.GetDateRange()
        startDate = pd.to_datetime(dateRange[0], format='%Y%m%d')
        dateRangeString = str(dateRange[0]) + '-' + str(dateRange[1])
        
        # create one record for each trip-stop, specific to the service
        # on this day
        tripList = schedule.GetTripList()            
            
        for trip in tripList:
            if trip.service_id == period.service_id:          
                # determine route attributes, and only keep bus trips
                route = schedule.GetRoute(trip.route_id)
                if (int(route.route_type) == self.BUS_ROUTE_TYPE):
                                            
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
                        
                    # get shape attributes, converted to a line
                    # this is needed because they are sometimes out of order
                    shape = schedule.GetShape(trip.shape_id)
                    shapeLine = self.getShapeLine(shape, stopTimeList)
                                                
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
                            
                            firstSeq = stopTime.stop_sequence
                
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
                            
                        # observations
                        record['TRIP_STOPS'] = 1
                        record['OBSERVED'] = 0
            
                        # For matching to AVL data
                        record['AGENCY_ID']        = str(route.agency_id).strip().upper()
                        record['ROUTE_SHORT_NAME'] = str(route.route_short_name).strip().upper()
                        record['ROUTE_LONG_NAME']  = str(route.route_long_name).strip().upper()
                        record['DIR']              = int(trip.direction_id)
                        record['TRIP']             = str(firstDeparture) + '_' + str(firstSeq)    # contains sequence and contains HHMM of departure from first stop
                        record['SEQ']              = int(stopTime.stop_sequence)                            
                            
                        # route/trip attributes
                        record['ROUTE_TYPE']       = int(route.route_type)
                        record['TRIP_HEADSIGN']    = str(trip.trip_headsign)
                        record['HEADWAY_S']        = np.NaN             # calculated below
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
                            runtime = max(0, round(timeDiff.total_seconds() / 60.0, 2))
                        record['RUNTIME_S'] = runtime
                        
                        # total time is sum of runtime and dwell time
                        tottime = runtime + dwellTime
                        record['TOTTIME_S'] = tottime
                            
                        # location along shape object (SFMTA uses meters)
                        if stopTime.shape_dist_traveled > 0: 
                            record['SHAPE_DIST'] = stopTime.shape_dist_traveled
                            distanceTraveled = stopTime.shape_dist_traveled * 3.2808399
                        else: 
                            x, y = convertLongitudeLatitudeToXY((stopTime.stop.stop_lon, stopTime.stop.stop_lat))
                            stopPoint = Point(x, y)
                            projectedDist = shapeLine.project(stopPoint, normalized=True)
                            distanceTraveled = shapeLine.length * projectedDist
                            record['SHAPE_DIST'] = distanceTraveled
    
                        # service miles
                        if startOfLine: 
                            serviceMiles = 0
                        else: 
                            serviceMiles = round((distanceTraveled - lastDistanceTraveled) / 5280.0, 3)
                        record['SERVMILES_S'] = serviceMiles
                                
                        # speed (mph)
                        if runtime > 0: 
                            record['RUNSPEED_S'] = round(serviceMiles / (runtime / 60.0), 2)
                        else:
                            record['RUNSPEED_S'] = 0
                            
                        if tottime > 0: 
                            record['TOTSPEED_S'] = round(serviceMiles / (tottime / 60.0), 2)
                        else:
                            record['TOTSPEED_S'] = 0
                                                    
    
                        # Additional GTFS IDs.        
                        record['ROUTE_ID']       = int(trip.route_id)
                        record['TRIP_ID']        = int(trip.trip_id)
                        record['STOP_ID']        = int(stopTime.stop.stop_id)
                        record['BLOCK_ID']       = int(trip.block_id)
                        record['SHAPE_ID']       = int(trip.shape_id)
                            
                        # indicates range this schedule is in operation    
                        record['SCHED_DATES'] = dateRangeString          # start and end date for this schedule
                        
                        # track from previous record
                        lastDepartureTime = departureTime      
                        lastDistanceTraveled = distanceTraveled                              
                                                                                                                                        
                        data.append(record)                
                        i += 1
                                    
        # convert to data frame and set unique index
        print "service_id %s has %i trip-stop records" % (period.service_id, len(data))
        df = pd.DataFrame(data)       
        df.index = pd.Series(range(0,len(df)))

        # calculate the headways, based on difference in previous bus on 
        # this route stopping at the same stop
        groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','SEQ']
        df = df.groupby(groupby, as_index=False).apply(calculateHeadways)
            
        # keep only relevant columns, sorted
        df.sort(indexColumns, inplace=True)                        
        df = df[colnames]
        
        return df
        
    
    def joinSFMuniData(self, gtfs, sfmuni):
        """
        Left join from GTFS to SFMuni sample.        
        
        gtfs_file - HDF file containing processed GTFS data      
        sfmuni_file - HDF file containing processed, just for sampled routes
        joined_outfile - HDF file containing merged GTFS and SFMuni data     
        """
        
        # convert column specs 
        colnames = []   
        indexColumns = []
        joinFields = []
        sources = {}
        for col in self.COLUMNS: 
            name = col[0]
            index = col[2]
            source = col[3]
            
            colnames.append(name)
            sources[name] = source
            if index==1: 
                indexColumns.append(name)
            if source=='join': 
                joinFields.append(name)
        

        sfmuni['OBSERVED'] = 1

        # join 
        try: 
            joined = pd.merge(gtfs, sfmuni, how='left', on=joinFields, 
                                    suffixes=('', '_AVL'), sort=True)
        except KeyError:
            print joinFields
            print gtfs.info()
            print gtfs.head()
            print sfmuni.info()
            print sfmuni.head()
            raise

        # calculate other derived fields
        # observations
        joined['OBSERVED'] = np.where(joined['OBSERVED_AVL'] == 1, 1, 0)

        # normalize to consistent measure of service miles
        joined['SERVMILES'] = np.where(joined['OBSERVED']==1, joined['SERVMILES_S'], np.nan)
        
        # schedule deviation          
        arrTime = pd.Series(zip(joined['ARRIVAL_TIME'], joined['ARRIVAL_TIME_S']), index=joined.index)   
        depTime = pd.Series(zip(joined['DEPARTURE_TIME'], joined['DEPARTURE_TIME_S']), index=joined.index)         
        joined['ARRIVAL_TIME_DEV']   = arrTime.apply(getScheduleDeviation)
        joined['DEPARTURE_TIME_DEV'] = depTime.apply(getScheduleDeviation)
        
        # ontime defined consistent with TCRP 165
        joined['ONTIME5'] = np.where((joined['DEPARTURE_TIME_DEV']>-1.0) & (joined['ARRIVAL_TIME_DEV']<5.0), 1, 0)
        joined['ONTIME5'] = joined['ONTIME5'].mask(joined['OBSERVED']==0, other=np.nan)
                                       
        # passenger miles traveled
        joined['PASSMILES'] = joined['LOAD_ARR'] * joined['SERVMILES']
                
        # passenger hours -- scheduled time
        joined['PASSHOURS'] = (joined['LOAD_ARR'] * joined['RUNTIME']
                             + joined['LOAD_DEP'] * joined['DWELL']).values / 60.0
                                                                                        
        # passenger hours of waiting time -- scheduled time
        joined['WAITHOURS'] = (joined['ON'] * 0.5 * joined['HEADWAY_S']).values / 60.0                    
                                   
        # fair paid, if each boarding pays full fare
        joined['FULLFARE_REV'] = (joined['ON'] * joined['FARE']) 
                    
        # passenger hours of delay at departure
        joined['PASSDELAY_DEP'] = np.where(joined['DEPARTURE_TIME_DEV']>0, 
                                     joined['ON'] * joined['DEPARTURE_TIME_DEV'], 0)
        joined['PASSDELAY_DEP'] = joined['PASSDELAY_DEP'].mask(joined['OBSERVED']==0, other=np.nan)
        
        # passenger hours of delay at arrival
        joined['PASSDELAY_ARR'] = np.where(joined['ARRIVAL_TIME_DEV']>0, 
                                     joined['ON'] * joined['ARRIVAL_TIME_DEV'], 0)
        joined['PASSDELAY_ARR'] = joined['PASSDELAY_ARR'].mask(joined['OBSERVED']==0, other=np.nan)

        # volume-capacity ratio
        joined['VC'] = (joined['LOAD_ARR']).values / (joined['CAPACITY']).values
                
        # croweded if VC>0.85
        # the capacity is the 'crush' load, so we are defining
        # crowding as 85% of that capacity.  In TCRP 165, this 
        # corresponds approximately to the range of 125-150% of
        # the seated load, which is the maximum design load for
        # peak of the peak conditions. 
        joined['CROWDED'] = np.where(joined['VC'] > 0.85, 1.0, 0.0)
        joined['CROWDED'] = joined['CROWDED'].mask(joined['OBSERVED']==0, other=np.nan)

        joined['CROWDHOURS'] = (joined['CROWDED'] * (
                                joined['LOAD_ARR'] * joined['RUNTIME']
                                + joined['LOAD_DEP'] * joined['DWELL'])).values / 60.0                       
                                                
        # keep only relevant columns, sorted
        joined.sort(indexColumns, inplace=True)                        
        joined = joined[colnames]
            
        return joined

    
    def weightTrips(self, trips):
        """
        Adds a series of weight columns to the trip df based on the ratio
        of total to observed trips.        
        """
        
        # start with all observations weighted equally
        trips['TRIPS'] = 1
        trips['TRIP_WEIGHT'] = trips['OBSERVED'].mask(trips['OBSERVED']==0, other=np.nan)
    
        # add the weight columns, specific to the level of aggregation
        # the weights build upon the lower-level weights, so we scale
        # the low-weights up uniformly within the group.  
                                        
        # routes
        trips['TOD_WEIGHT'] = calcWeights(trips, 
                groupby=['DATE','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                oldWeight='TRIP_WEIGHT')
    
        trips['DAY_WEIGHT'] = calcWeights(trips, 
                groupby=['DATE','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                oldWeight='TOD_WEIGHT')
        
        # system
        trips['SYSTEM_WEIGHT'] = calcWeights(trips, 
                groupby=['DATE','TOD','AGENCY_ID'], 
                oldWeight='DAY_WEIGHT')
                
        return trips
                        
        
    def getStringLengths(self, usedColumns):
        """
        gets the max string length for the columns that are in use
        """
        
        # convert column specs 
        stringLengths= {}
        for col in self.COLUMNS: 
            name = col[0]
            if name in usedColumns: 
                stringLength = col[1]
                if (stringLength>0): 
                    stringLengths[name] = stringLength
                
        return stringLengths

    def getShapeLine(self, shape, stopTimeList):
        """
        Accepts a shape and a list of stop times in GTFS transit feed format.
        
        Returns a LineString object of the shape in the appropriate order.
        
        This is needed because the points in the shapes are sometimes in 
        a scrambled order in the input files. 
        """
        
        # first create a LineString from the stops, which are in the right order
        stopPoints = []
        for stopTime in stopTimeList:
            x, y = convertLongitudeLatitudeToXY((stopTime.stop.stop_lon, stopTime.stop.stop_lat))
            stopPoints.append((x, y))
        
        if len(stopPoints)>1: 
            stopLine = LineString(stopPoints)
        
        # then project each point onto that stopLine
        shapePointDict = {}
        for p in shape.points: 
            x, y = convertLongitudeLatitudeToXY((p[1], p[0]))
            if len(stopPoints)>1: 
                projectedDist = stopLine.project(Point(x, y), normalized=True)
            else:                
                projectedDist = p[2]
            shapePointDict[projectedDist] = (x,y)
        
        # now order by the projected distance, and create the shape
        shapePoints = []
        for key in sorted(shapePointDict):
            shapePoints.append(shapePointDict[key])
        shapeLine = LineString(shapePoints)
        
        return shapeLine    
        
        
        