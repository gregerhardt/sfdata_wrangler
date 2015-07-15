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
import glob

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
    ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','SEQ']
    (but not by TRIP).     
    """        
    df.sort(['DEPARTURE_TIME_S'], inplace=True)

    lastDeparture = 0
    for i, row in df.iterrows():    
        if lastDeparture==0: 
            df['HEADWAY_S'][i] = np.NaN        # missing headway for first trip
        else:
            diff = df['DEPARTURE_TIME_S'][i] - lastDeparture
            df['HEADWAY_S'][i] = round(diff.seconds / 60.0, 2)
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


def getOutfile(filename, date):
    """
    gets a filename with the year replacing YYYY
    """
    return filename.replace('YYYY', str(date.year))


def getOutkey(month, dow):
    """
    gets the key name as a string from the month and the day of week
    """
    return 'm' + str(month.date()).replace('-', '') + 'd' + str(dow)
                    

def calcGroupWeights(df, oldWeight):
    """
    df - dataframe to operate on.  Must contain columns for TRIP_STOPS
         and for the oldWeight.  
    oldWeight - column name in df containing the previous weight. 
    
    Intended to operate on a group and weight up to the TRIP_STOPS
    in the group.  
    
    """
    
    obs = float((df[oldWeight] * df['TRIP_STOPS']).sum())
    tot = float(df['TRIP_STOPS'].sum())    
    if obs>0:
        factor = tot / obs
    else: 
        factor = np.nan
        
    out = df[oldWeight] * factor   
    
    return out


def calcWeights(df, groupby, oldWeight):
    """
    df - dataframe to operate on.  Must contain columns for TOTTRIPS
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
	['SERVMILES_S' ,      0, 0, 'gtfs'], 
	['SERVMILES',         0, 0, 'avl'],         # Distances and speeds
	['RUNSPEED_S' ,       0, 0, 'gtfs'], 
	['RUNSPEED'   ,       0, 0, 'calculated'], 
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
        ['SCHED_START',       0, 0, 'gtfs'],  # range of this GTFS schedule
        ['SCHED_END',         0, 0, 'gtfs']
        ]
                

    def processRawData(self, gtfs_file, sfmuni_file, expanded_file):
        """
        Read GTFS, cleans it, processes it, and writes it to an HDF5 file.
        This will be done for every individual day, so you get a list of 
        every bus that runs. 
        
        infile  - in GTFS format
        outfile - output file name in h5 format, same as AVL/APC format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', gtfs_file
        
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
                        
        # open the data stores
        sfmuni_store = pd.HDFStore(sfmuni_file)
        
        # establish the feed
        tfl = transitfeed.Loader(feed_path=gtfs_file)
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
                    # determine route attributes, and only keep bus trips
                    route = schedule.GetRoute(trip.route_id)
                    if (int(route.route_type) == self.BUS_ROUTE_TYPE):
                        
                        
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
                            
                            # observations
                            record['TRIP_STOPS'] = 1
                            record['OBSERVED'] = 0
            
                            # For matching to AVL data
                            record['AGENCY_ID']        = str(route.agency_id).strip().upper()
                            record['ROUTE_SHORT_NAME'] = str(route.route_short_name).strip().upper()
                            record['ROUTE_LONG_NAME']  = str(route.route_long_name).strip().upper()
                            record['DIR']              = int(trip.direction_id)
                            record['TRIP']             = firstDeparture    # contains HHMM of departure from first stop
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
                            record['SERVMILES_S'] = serviceMiles
                                
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
                                    
            # convert to data frame and set unique index
            print "service_id %s has %i trip-stop records" % (period.service_id, len(data))
            df = pd.DataFrame(data)       
            df.index = pd.Series(range(0,len(df)))

            # calculate the headways, based on difference in previous bus on 
            # this route stopping at the same stop
            groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','SEQ']
            df = df.groupby(groupby).apply(calculateHeadways)
            df = df.drop(['AGENCY_ID','ROUTE_SHORT_NAME','DIR','SEQ'], axis=1)
            df = df.reset_index()
            
            # keep only relevant columns, sorted
            df.sort(indexColumns, inplace=True)                        
            df = df[colnames]
            
            # keep one dataframe for each service period
            dataframes[period.service_id] = df


        # loop through each date, and add the appropriate service to the database
        print 'Writing data for periods from ', startDate, ' to ', endDate

        servicePeriodsEachDate = schedule.GetServicePeriodsActiveEachDate(startDate, endDate)        
        for date, servicePeriods in servicePeriodsEachDate:
            
            month = ((pd.to_datetime(date)).to_period('month')).to_timestamp()    
  
            for period in servicePeriods: 
                
                # get the corresponding MUNI data for this date
                sfmuni = sfmuni_store.select('sample', where='DATE==Timestamp(date)')
                
                if len(sfmuni)>0: 
                    print 'Writing ', date, ' with ', len(sfmuni), ' observed records.'
            
                    df = dataframes[period.service_id]
                    
                    # update the dates
                    for i, row in df.iterrows():
                        df.at[i,'ARRIVAL_TIME_S'] = date + (row['ARRIVAL_TIME_S'] - row['DATE'])
                        df.at[i,'DEPARTURE_TIME_S'] = date + (row['DEPARTURE_TIME_S'] - row['DATE'])
                    df['DATE'] = date
                    df['MONTH'] = month
                    
                    # join the sfmuni data
                    joined = self.joinSFMuniData(df, sfmuni)            
    
                    # write the output        
                    # use a separate file for each year
                    # and write a separate table for each month and DOW
                    # format of the table name is YYYYMMDDdX, where X is the day of week
                    outfile = getOutfile(expanded_file, month)
                    outkey = getOutkey(month, period.service_id)                    
                    outstore = pd.HDFStore(outfile)         
                                            
                    outstore.append(outkey, joined, data_columns=True, 
                                min_itemsize=stringLengths)
                    
                    outstore.close()

        sfmuni_store.close()


    def joinSFMuniData(self, gtfs, sfmuni):
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
        

        sfmuni['OBSERVED'] = 1

        # join 
        joined = pd.merge(gtfs, sfmuni, how='left', on=joinFields, 
                                suffixes=('', '_AVL'), sort=True)

        # initialize derived fields as missing
        joined['RUNTIME'] = np.NaN
        joined['RUNSPEED'] = np.NaN
        joined['ARRIVAL_TIME_DEV']   = np.NaN
        joined['DEPARTURE_TIME_DEV'] = np.NaN
        joined['ONTIME5']  = np.NaN
        joined['PASSMILES']   = np.NaN
        joined['PASSHOURS']   = np.NaN
        joined['WAITHOURS']   = np.NaN
        joined['FULLFARE_REV']   = np.NaN
        joined['PASSDELAY_DEP'] = np.NaN
        joined['PASSDELAY_ARR'] = np.NaN
        joined['VC'] = np.NaN
        joined['CROWDED'] = np.NaN
        joined['CROWDHOURS'] = np.NaN

        # calculate derived fields, in overlapping frames           
        lastRoute = 0
        lastDir = 0
        lastTrip = 0
        lastDepartureTime = 0
        for i, row in joined.iterrows():
            if row['OBSERVED_AVL'] == 1: 
                joined.at[i,'OBSERVED'] = 1
                
                # observed runtime
                if (row['ROUTE_AVL']==lastRoute 
                    and row['DIR']==lastDir 
                    and row['TRIP']==lastTrip): 

                    diff = row['ARRIVAL_TIME'] - lastDepartureTime
                    runtime = max(0, round(diff.total_seconds() / 60.0, 2))
                else: 
                    runtime = 0
                        
                    lastRoute = row['ROUTE_AVL']
                    lastDir = row['DIR']
                    lastTrip = row['TRIP']
                        
                joined.at[i,'RUNTIME'] = runtime
                lastDepartureTime = row['DEPARTURE_TIME']

                # observed speed
                if runtime>0: 
                    joined.at[i,'RUNSPEED'] = round(row['SERVMILES_S'] / (runtime / 60.0), 2)
                else: 
                    joined.at[i,'RUNSPEED'] = 0
                    
            
                # deviation from scheduled arrival
                if row['ARRIVAL_TIME'] >= row['ARRIVAL_TIME_S']: 
                    diff = row['ARRIVAL_TIME'] - row['ARRIVAL_TIME_S']
                    arrivalTimeDeviation = round(diff.seconds / 60.0, 2)
                else: 
                    diff = row['ARRIVAL_TIME_S'] - row['ARRIVAL_TIME']
                    arrivalTimeDeviation = - round(diff.seconds / 60.0, 2)                        
                joined.at[i,'ARRIVAL_TIME_DEV'] = arrivalTimeDeviation
    
                # deviation from scheduled departure
                if row['DEPARTURE_TIME'] >= row['DEPARTURE_TIME_S']: 
                    diff = row['DEPARTURE_TIME'] - row['DEPARTURE_TIME_S']
                    departureTimeDeviation = round(diff.seconds / 60.0, 2)
                else: 
                    diff = row['DEPARTURE_TIME_S'] - row['DEPARTURE_TIME']
                    departureTimeDeviation = - round(diff.seconds / 60.0, 2)                        
                joined.at[i,'DEPARTURE_TIME_DEV'] = departureTimeDeviation
                
                # ontime, from -1 to 5 minutes
                # Consistent with definition in TCRP 165
                if (arrivalTimeDeviation>-1.0 and arrivalTimeDeviation < 5.0): 
                    joined.at[i,'ONTIME5'] = 1
                else: 
                    joined.at[i,'ONTIME5'] = 0
                                
                # passenger miles traveled
                joined.at[i,'PASSMILES'] = row['LOAD_ARR'] * row['SERVMILES_S']
                
                # passenger hours -- scheduled time
                joined.at[i,'PASSHOURS'] = (row['LOAD_ARR'] * row['RUNTIME_S']
                                        + row['LOAD_DEP'] * row['DWELL_S']) / 60.0
                                                                                        
                # passenger hours of waiting time -- scheduled time
                joined.at[i,'WAITHOURS'] = (row['ON'] 
                                    * 0.5 * row['HEADWAY_s']) / 60.0                    
                                   
                # fair paid, if each boarding pays full fare
                joined.at[i,'FULLFARE_REV'] = (row['ON'] * row['FARE']) 
                    
                # passenger hours of delay at departure
                if departureTimeDeviation > 0: 
                    joined.at[i,'PASSDELAY_DEP'] = (row['ON']
                                        * departureTimeDeviation) / 60.0
                else: 
                    joined.at[i,'PASSDELAY_DEP'] = 0                    
                
                # passenger hours of delay at arrival  
                if arrivalTimeDeviation > 0: 
                    joined.at[i,'PASSDELAY_ARR'] = (row['OFF'] 
                                        * arrivalTimeDeviation) / 60.0
                else: 
                    joined.at[i,'PASSDELAY_ARR'] = 0        
                
                # volume-capacity ratio
                joined.at[i,'VC'] = row['LOAD_ARR'] / row['CAPACITY']
                
                # croweded if VC>0.85
                # the capacity is the 'crush' load, so we are defining
                # crowding as 85% of that capacity.  In TCRP 165, this 
                # corresponds approximately to the range of 125-150% of
                # the seated load, which is the maximum design load for
                # peak of the peak conditions. 
                if (row['LOAD_ARR'] / row['CAPACITY'] > 0.85):
                    joined.at[i,'CROWDED'] = 1.0
                    joined.at[i,'CROWDHOURS'] = (row['LOAD_ARR'] * row['RUNTIME_S']
                                               + row['LOAD_DEP'] * row['DWELL_S']) / 60.0                  
                else: 
                    joined.at[i,'CROWDED'] = 0.0
                    joined.at[i,'CROWDHOURS'] = 0.0   
                    
                        
                                                
            # keep only relevant columns, sorted
        joined.sort(indexColumns, inplace=True)                        
        joined = joined[colnames]
            
        return joined

    
    def weightExpandedData(self, expanded_file, weighted_file):
        """
        Reads in the expanded sfmuni data, and adds a series of weight columns
        that will be used when aggregating the data.  
        
        """
        
        # get all infiles matching the pattern
        pattern = expanded_file.replace('YYYY', '*')
        infiles = glob.glob(pattern)
        print 'Retrieved a total of %i years to process' % len(infiles)
        
        for infile in infiles: 
            
            # open the data store and get the tokens to loop through
            instore = pd.HDFStore(infile)            
            keys = instore.keys()
            print 'Retrieved a total of %i keys to process' % len(keys)   
    
            # loop through the months, and days of week
            for key in keys: 
                print 'Processing ', key
                
                # get a months worth of data for this day of week
                # be sure we have a clean index
                df = instore.select(key)                        
                df.index = pd.Series(range(0,len(df)))      
                    
                # start with all observations weighted equally
                df['TRIP_STOPS'] = 1
                df['BASE_WEIGHT'] = df['OBSERVED'].mask(df['OBSERVED']==0, other=np.nan)
    
                # add the weight columns, specific to the level of aggregation
                # the weights build upon the lower-level weights, so we scale
                # the low-weights up uniformly within the group.  
                    
                # route_stops    
                df['RS_TOD_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                        oldWeight='BASE_WEIGHT')                
                                                
                df['RS_DAY_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                        oldWeight='RS_TOD_WEIGHT')
                    
                # routes
                df['ROUTE_TOD_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                        oldWeight='RS_TOD_WEIGHT')
    
                df['ROUTE_DAY_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                        oldWeight='ROUTE_TOD_WEIGHT')
    
                # stops
                df['STOP_TOD_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','TOD','AGENCY_ID','STOP_ID'], 
                        oldWeight='RS_TOD_WEIGHT')
    
                df['STOP_DAY_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','AGENCY_ID','STOP_ID'], 
                        oldWeight='STOP_TOD_WEIGHT')
    
                # system
                df['SYSTEM_TOD_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','TOD','AGENCY_ID'], 
                        oldWeight='ROUTE_TOD_WEIGHT')
    
                df['SYSTEM_DAY_WEIGHT'] = calcWeights(df, 
                        groupby=['DATE','AGENCY_ID'], 
                        oldWeight='ROUTE_DAY_WEIGHT')
                            
                # use a separate file for each year
                # and write a separate table for each month and DOW
                # format of the table name is YYYYMMDDdX, where X is the day of week
                datestring = (key.partition('m')[2]).partition('d')[0]
                month = pd.to_datetime(datestring, format='%Y%m%d')
                outfile = getOutfile(weighted_file, pd.Timestamp(month))
                    
                outstore = pd.HDFStore(outfile)
                    
                # don't append the data, overwrite
                try: 
                    outstore.remove(key)      
                    print "  Replacing HDF table ", key       
                except KeyError: 
                    print "  Creating HDF table ", key
                        
                outstore.append(key, df, data_columns=True)
                outstore.close()
            
            instore.close()
