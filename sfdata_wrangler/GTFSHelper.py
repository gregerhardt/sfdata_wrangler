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

import transitfeed
            
            
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
        
                                                
            
class GTFSHelper():
    """ 
    Methods used for loading and converting General Transit Feed Specification
    (GTFS) files, and relating them to AVL/APC data. 
    
    """

    # specifies how to read in each column from raw input files
    #  columnName,       stringLength, index(0/1)
    COLUMNS = [
        ['DATE',              0, 1],  # Calendar attributes
        ['DOW',               0, 1], 
        ['TOD',               0, 0],
        ['AGENCY_ID',        10, 0],  # for matching to AVL data
        ['ROUTE_SHORT_NAME', 32, 1], 
        ['ROUTE_LONG_NAME',  32, 1], 
        ['DIR',               0, 1], 
        ['TRIP',              0, 1], 
        ['SEQ',               0, 1], 
        ['ROUTE_ID',          0, 0],  # additional GTFS IDs
        ['TRIP_ID',           0, 0], 
        ['STOP_ID',           0, 0], 
        ['BLOCK_ID',          0, 0], 
        ['SHAPE_ID',          0, 0],
        ['ROUTE_TYPE',        0, 0], 
        ['TRIP_HEADSIGN',    32, 0], 
        ['FARE',              0, 0], 
        ['STOPNAME',         32, 0], 
        ['STOP_LAT',          0, 0], 
        ['STOP_LON',          0, 0], 
        ['SOL',               0, 0],
        ['EOL',               0, 0],
        ['ARRIVAL_TIME_S',    0, 0], 
        ['DEPARTURE_TIME_S',  0, 0]
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
            
            colnames.append(name)
            if (stringLength>0): 
                stringLengths[name] = stringLength
            if index==1: 
                indexColumns.append(name)

        # open the data store
        store = pd.HDFStore(outfile)
                
        # create an empty list of dictionaries to store the data
        data = []
        
        # establish the feed
        tfl = transitfeed.Loader(feed_path=infile)
        schedule = tfl.Load()

        # determine the dates, and corresponding service on each date
        dateRange = schedule.GetDateRange()
        startDate = pd.to_datetime(dateRange[0], format='%Y%m%d')
        endDate   = pd.to_datetime(dateRange[1], format='%Y%m%d')
        servicePeriodsEachDate = schedule.GetServicePeriodsActiveEachDate(startDate, endDate)
        
        # loop through each date to create detailed records on every day
        for date, servicePeriods in servicePeriodsEachDate:
            for period in servicePeriods: 
                print 'Processing ', date
        
                # create one record for each trip-stop, specific to the service
                # on this day
                tripList = schedule.GetTripList()
                for trip in tripList:
                    if trip.service_id == period.service_id:
                        
                        # determine route attributes
                        route = schedule.GetRoute(trip.route_id)
                        
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
                        i = 0        
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
            
                            else:
                                startOfLine = 0
                                
                            if i==(len(stopTimeList)-1):
                                endOfLine = 1
                            else: 
                                endOfLine = 0
                            
                            # calendar attributes
                            record['DATE'] = date
                            record['DOW']  = int(trip.service_id)
                            record['TOD']  = timeOfDay
            
                            # For matching to AVL data
                            record['AGENCY_ID']        = str(route.agency_id)
                            record['ROUTE_SHORT_NAME'] = str(route.route_short_name)
                            record['ROUTE_LONG_NAME']  = str(route.route_long_name)
                            record['DIR']              = int(trip.direction_id)
                            record['TRIP']             = firstDeparture    # contains HHMM of departure from first stop
                            record['SEQ']              = int(stopTime.stop_sequence)
                            
                            # Additional GTFS IDs. 
                            record['ROUTE_ID']       = int(trip.route_id)
                            record['TRIP_ID']        = int(trip.trip_id)
                            record['STOP_ID']        = int(stopTime.stop.stop_id)
                            record['BLOCK_ID']       = int(trip.block_id)
                            record['SHAPE_ID']       = int(trip.shape_id)
                            
                            # route/trip attributes
                            record['ROUTE_TYPE']       = int(route.route_type)
                            record['TRIP_HEADSIGN']    = str(trip.trip_headsign)
                            record['FARE']             = float(fare)  
                            
                            # stop attriutes
                            record['STOPNAME']         = str(stopTime.stop.stop_name)
                            record['STOP_LAT']         = float(stopTime.stop.stop_lat)
                            record['STOP_LON']         = float(stopTime.stop.stop_lon)
                            record['SOL']              = startOfLine
                            record['EOL']              = endOfLine
                            
                            # stop times, dealing with wrap-around for times past midnight            
                            record['ARRIVAL_TIME_S']   = getWrapAroundTime(str(date.date()), stopTime.arrival_time)
                            record['DEPARTURE_TIME_S'] = getWrapAroundTime(str(date.date()), stopTime.departure_time)
                            
                            data.append(record)                
                            i += 1
                                
            # convert to data frame
            print "  adding %i trip-stop records" % len(data)
            df = pd.DataFrame(data)
            
            # sort rows 
            df.sort(indexColumns, inplace=True)
                    
            # keep only relevant columns, sorted
            df = df[colnames]
            
            # write the data
            try: 
                store.append('gtfs', df, data_columns=True, 
                        min_itemsize=stringLengths)
            except ValueError: 
                store = pd.HDFStore(outfile)
                print 'Structure of HDF5 file is: '
                print store.gtfs.dtypes
                store.close()
                    
                print 'Structure of current dataframe is: '
                print df.dtypes
                    
                raise

        store.close()

