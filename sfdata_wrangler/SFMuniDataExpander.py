
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

import pandas as pd
import numpy as np
import datetime

from SFMuniDataAggregator import SFMuniDataAggregator
from GTFSHelper import GTFSHelper
            
            
    
def calculateRuntime(df):
    """
    Calculates the runtime between trip_stops. Assumes data are grouped by: 
    ['AGENCY_ID','ROUTE_SHORT_NAME','DIR','TRIP']
    """        
    df.sort_values(['SEQ'], inplace=True)

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
        

def getScheduleDeviation(times):
    """
    Calculates schedule devation based on a tuple (actualTime, schedTime)
                                       
    """
	
    (actualTime, schedTime) = times
	
    if pd.isnull(actualTime): 
        return np.nan
    elif (actualTime >= schedTime):
        diff = actualTime - schedTime
        return round(diff.seconds / 60.0, 2)
    else: 
        diff = schedTime - actualTime
        return -round(diff.seconds / 60.0, 2)
        


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

    
def getInkey(month, prefix):
    """
    gets the key name as a string from the month
    """
    return prefix + str(month.date()).replace('-', '')
    
    
def calcGroupWeights(df, oldWeight):
    """
    df - dataframe to operate on.  Must contain columns for TRIPS
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
    

            
    
class SFMuniDataExpander():
    """ 
    Methods for expanding SFMuniData to the GTFS data and weighting it.  
    
    """

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
	['VEHNO'     ,        0, 0, 'avl'], 
    ['SCHED_DATES',      20, 0, 'gtfs']  # range of this GTFS schedule
    ]
                
    

    def __init__(self, gtfs_outfile, trip_outfile, ts_outfile, 
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
        self.gtfs_store = pd.HDFStore(gtfs_outfile)
        
        # which days of week to run for
        self.dow = dow
        
        # helper for creating data aggregations
        self.aggregator = SFMuniDataAggregator(daily_trip_outfile=daily_trip_outfile, 
                                               daily_ts_outfile=daily_ts_outfile)
        
        # count the trips and trip-stops to ensure a unique index
        self.tripCount = startingTripCount
        self.tsCount = startingTsCount
        
        # running a specific range 
        self.startDate = startDate
        self.endDate = endDate
        print('Running expansion for date rage ', self.startDate, ' to ', self.endDate)
    
    
    def closeStores(self):  
        """
        Closes all datastores. 
        """
        self.sfmuni_store.close()
        self.gtfs_store.close()
        self.aggregator.close()
        
    def expandAndWeight(self, gtfs_file, sfmuni_file):
        """
        Read GTFS, cleans it, processes it, and writes it to an HDF5 file.
        This will be done for every individual day, so you get a list of 
        every bus that runs. 
        
        infile  - in GTFS format
        outfile - output file name in h5 format, same as AVL/APC format
        """
        
        print(datetime.datetime.now().ctime(), 'Converting raw data in file: ', gtfs_file)
              
        # establish the feed, reading only the bus routes
        gtfsHelper = GTFSHelper()
        gtfsHelper.establishTransitFeed(gtfs_file)
        
        # get the date ranges
        gtfsDateRange = gtfsHelper.schedule.GetDateRange()        
        gtfsStartDate = pd.to_datetime(gtfsDateRange[0], format='%Y%m%d')
        gtfsEndDate   = pd.to_datetime(gtfsDateRange[1], format='%Y%m%d')
        dateRangeString = str(gtfsDateRange[0]) + '-' + str(gtfsDateRange[1])
                
        # create dictionary with one dataframe for each service period
        # read these from the GTFS file that was previously created
        dataframes = {}
        servicePeriods = gtfsHelper.schedule.GetServicePeriodList()        
        for period in servicePeriods:   
            service_id = period.service_id
            if int(service_id) in self.dow:                
                # only keep the busses here
                print('Reading service_id ', service_id)
                dataframes[service_id] = self.gtfs_store.select('sfmuni', 
                           where="SCHED_DATES=dateRangeString & SERVICE_ID=service_id & ROUTE_TYPE=3")
            
        # note that the last date is not included, hence the +1 increment
        servicePeriodsEachDate = gtfsHelper.schedule.GetServicePeriodsActiveEachDate(gtfsStartDate, gtfsEndDate + pd.DateOffset(days=1)) 
                   
        # loop through each date, and add the appropriate service to the database  
        print('Writing data for periods from ', gtfsStartDate, ' to ', gtfsEndDate)
        for date, servicePeriodsForDate in servicePeriodsEachDate:           
                        
            if (pd.Timestamp(date)>=pd.Timestamp(self.startDate)) and (pd.Timestamp(date)<=pd.Timestamp(self.endDate)):           
            
                print(datetime.datetime.now().ctime(), ' Processing ', date)         
                
                # use a separate file for each year
                # and write a separate table for each month and DOW
                # format of the table name is mYYYYMMDDdX, where X is the day of week
                month = ((pd.to_datetime(date)).to_period('M')).to_timestamp()    
                trip_outstore = pd.HDFStore(getOutfile(self.trip_outfile, month))  
                ts_outstore = pd.HDFStore(getOutfile(self.ts_outfile, month))  
                
                for period in servicePeriodsForDate: 
                    if int(period.service_id) in self.dow:     
                        
                        outkey = getOutkey(month=month, dow=period.service_id, prefix='m')                                             
    
                        # get the corresponding MUNI data for this date, and only continue if there 
                        # are observed values
                        sfmuni = self.getSFMuniData(sfmuni_file, date)                        
                        if len(sfmuni) > 0: 
                        
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
                            
                            # not sure why it thinks SEQ is an object and not an int, but try converting
                            ts['SEQ'] = ts['SEQ'].astype('int64')
                            
                            # write the trip-stops             
                            stringLengths = self.getStringLengths(ts.columns)   
                            ts_outstore.append(outkey, ts, data_columns=True, 
                                            min_itemsize=stringLengths)                            

                            # aggregate to TOD and daily totals, and write those
                            self.aggregator.aggregateTripsToDays(trips)
                            self.aggregator.aggregateTripStopsToDays(ts)
                        
                                                
                trip_outstore.close()
                ts_outstore.close()

    def getSFMuniData(self, sfmuni_file, date):
        """
        Returns a dataframe with the observed SFMuni records
        and some processing of those
        """

        # use a separate file for each year
        # and write a separate table for each month and DOW
        # format of the table name is mYYYYMMDDdX, where X is the day of week
        month = ((pd.to_datetime(date)).to_period('M')).to_timestamp()    
        sfmuni_store = pd.HDFStore(getOutfile(sfmuni_file, month))
        sfmuni_key = getInkey(month, 'm')
                
        sfmuni = sfmuni_store.select(sfmuni_key, where='DATE==Timestamp(date)')
        sfmuni.index = pd.Series(range(0,len(sfmuni)))
        
        # drop duplicates, which would get double-counted
        sfmuni = sfmuni.drop_duplicates(subset=['AGENCY_ID','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP', 'SEQ'])
                
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
            print(joinFields)
            print(gtfs.info())
            print(gtfs.head())
            print(sfmuni.info())
            print(sfmuni.head())
            raise

        # calculate other derived fields
        # observations
        joined['OBSERVED'] = np.where(joined['OBSERVED_AVL'] == 1, 1, 0)

        # normalize to consistent measure of service miles
        joined['SERVMILES'] = joined['SERVMILES_S']
        
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
        joined.sort_values(indexColumns, inplace=True)           

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
