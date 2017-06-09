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

import sys
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
    
    
def getDayOfWeek(service_id):
    """
    determine the day-of-week as 1=weekday, 2=sat, 3=sun
    """ 
    try: 
        dow = int(service_id)
    except ValueError: 
        if service_id=='WKDY' or service_id=='M-FSAT': 
            dow = 1
        elif service_id=='SAT': 
            dow = 2
        elif service_id=='SUN' or service_id=='SUNAB': 
            dow = 3
        else:
            raise
    return dow
    

class GTFSHelper():
    """ 
    Methods used for loading and converting General Transit Feed Specification
    (GTFS) files
    
    """

    STRING_LENGTHS = {
        'AGENCY_ID'       : 10,  
        'TOD'             : 10,    
        'ROUTE_SHORT_NAME': 32,  
        'ROUTE_LONG_NAME' : 40,  
        'TRIP'            : 10,  
        'DIR'             : 10,  
        'TRIP_HEADSIGN'   : 64,  
        'STOPNAME'        : 40,  
        'SCHED_DATES'     : 20, 
        'ROUTE_ID'        : 10,  
        'TRIP_ID'         : 12,  
        'STOP_ID'         : 10,  
        'SERVICE_ID'      : 10,  
        }

    def __init__(self):
        """
        Constructor.                 
        """       
        self.schedule = None
        
    
    def establishTransitFeed(self, gtfs_file): 
        """
        Sets up the transit feed. 
        """
        tfl = transitfeed.Loader(feed_path=gtfs_file)
        self.schedule = tfl.Load()
        
        
    def processFiles(self, infiles, outfile, outkey):
        """
        Processes the list of GTFS files and stores
        them in an HDF format.   
        """
        
        outstore = pd.HDFStore(outfile) 
        if '/' + outkey in outstore.keys(): 
            outstore.remove(outkey)
           
        startIndex = 0
        
        for infile in infiles: 
            print ('\n\nReading ', infile)
            
            self.establishTransitFeed(infile)
            servicePeriods = self.schedule.GetServicePeriodList()        
            for period in servicePeriods:   
                
                df = self.getGTFSDataFrame(period, startIndex)       
                
                outstore.append(outkey, df, data_columns=True, 
                    min_itemsize=self.STRING_LENGTHS)

                startIndex += len(df)
        
        outstore.close()

        self.createDailySystemTotals(infiles, outfile, outkey, outkey+'Daily')
        self.createMonthlySystemTotals(outfile,outkey+'Daily',outkey+'Monthly')
        
        

    def createDailySystemTotals(self, infiles, outfile, inkey, outkey):
        """
        Converts from the detailed schedule information to the 
        daily system totals.
        
        """
        
        outstore = pd.HDFStore(outfile) 
        if '/' + outkey in outstore.keys(): 
            outstore.remove(outkey)

        # determine the system totals, grouped by schedule dates
        detailed_df = outstore.get(inkey)
        aggregator = SFMuniDataAggregator()        
        AGGREGATION_RULES = [            
           	['TRIPS'        ,'TRIP_ID'     ,aggregator.countUnique, 'system', 'int64', 0],
           	['STOPS'        ,'STOP_ID'     ,aggregator.countUnique, 'system', 'int64', 0],
           	['TRIP_STOPS'   ,'TRIP_STOPS'  ,'sum',  'system', 'int64', 0],
           	['FARE'         ,'FARE'        ,'mean', 'system', 'float64', 0],
           	['HEADWAY_S'    ,'HEADWAY_S'   ,'mean', 'system', 'float64', 0],
           	['SERVMILES_S'  ,'SERVMILES_S' ,'sum',  'system', 'float64', 0],
           	['DWELL_S'      ,'DWELL_S'     ,'sum',  'system', 'float64', 0],
           	['RUNTIME_S'    ,'RUNTIME_S'   ,'sum',  'system', 'float64', 0],
           	['TOTTIME_S'    ,'TOTTIME_S'   ,'sum',  'system', 'float64', 0],
           	['RUNSPEED_S'   ,'RUNSPEED_S'  ,'mean', 'system', 'float64', 0],
           	['TOTSPEED_S'   ,'TOTSPEED_S'  ,'mean', 'system', 'float64', 0]
                ]                
        aggdf, stringLengths  = aggregator.aggregateTransitRecords(detailed_df, 
                groupby=['SCHED_DATES','DOW','SERVICE_ID','AGENCY_ID','ROUTE_TYPE'], 
                columnSpecs=AGGREGATION_RULES)

        # use the GTFS files to determine the service in operation for each date
        for infile in infiles: 
            print ('\n\nReading ', infile)
            
            self.establishTransitFeed(infile)

            # loop through each date, and add the appropriate service to the database  
            gtfsDateRange = self.schedule.GetDateRange()
            dateRangeString = str(gtfsDateRange[0]) + '-' + str(gtfsDateRange[1])
            gtfsStartDate = pd.to_datetime(gtfsDateRange[0], format='%Y%m%d')
            gtfsEndDate   = pd.to_datetime(gtfsDateRange[1], format='%Y%m%d') 
            
            # note that the last date is not included, hence the +1 increment
            servicePeriodsEachDate = self.schedule.GetServicePeriodsActiveEachDate(gtfsStartDate, gtfsEndDate + pd.DateOffset(days=1)) 
            
            for date, servicePeriodsForDate in servicePeriodsEachDate:     
                print (' Processing ', date)
                
                # current month
                month = ((pd.to_datetime(date)).to_period('M')).to_timestamp() 
                
                # figure out the day of week based on the schedule in operation
                dow = 1
                for period in servicePeriodsForDate:   
                    servIdString = str(period.service_id).strip().upper()        
                    if servIdString=='SAT' or servIdString=='2': 
                        dow = 2
                    if servIdString=='SUN' or servIdString=='3': 
                        dow = 3
        
                # select and append the appropriate aggregated records for this date
                for period in servicePeriodsForDate:   
                        
                    servIdString = str(period.service_id).strip().upper()

                    records = aggdf[(aggdf['SCHED_DATES']==dateRangeString) & (aggdf['SERVICE_ID']==servIdString)]

                    records['DOW'] = dow
                    records['DATE'] = date
                    records['MONTH'] = month
                        
                    # write the data
                    outstore.append(outkey, records, data_columns=True, 
                            min_itemsize=stringLengths)

        outstore.close()


    def createMonthlySystemTotals(self,  outfile, inkey, outkey):
        """
        Converts from the detailed schedule information to the 
        daily system totals.
        
        """
        
        print ('Calculating monthly totals')
        
        outstore = pd.HDFStore(outfile) 
        if '/' + outkey in outstore.keys(): 
            outstore.remove(outkey)

        # determine the system totals, grouped by schedule dates
        df = outstore.get(inkey)
        aggregator = SFMuniDataAggregator()        
        AGGREGATION_RULES = [            
           	['TRIPS'        ,'TRIPS'       ,'mean', 'system', 'int64',   0],
           	['STOPS'        ,'STOPS'       ,'mean', 'system', 'int64',   0],
           	['TRIP_STOPS'   ,'TRIP_STOPS'  ,'mean', 'system', 'int64',   0],
           	['FARE'         ,'FARE'        ,'mean', 'system', 'float64', 0],
           	['HEADWAY_S'    ,'HEADWAY_S'   ,'mean', 'system', 'float64', 0],
           	['SERVMILES_S'  ,'SERVMILES_S' ,'mean', 'system', 'float64', 0],
           	['DWELL_S'      ,'DWELL_S'     ,'mean', 'system', 'float64', 0],
           	['RUNTIME_S'    ,'RUNTIME_S'   ,'mean', 'system', 'float64', 0],
           	['TOTTIME_S'    ,'TOTTIME_S'   ,'mean', 'system', 'float64', 0],
           	['RUNSPEED_S'   ,'RUNSPEED_S'  ,'mean', 'system', 'float64', 0],
           	['TOTSPEED_S'   ,'TOTSPEED_S'  ,'mean', 'system', 'float64', 0]
                ]                
        aggdf, stringLengths  = aggregator.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID','ROUTE_TYPE'], 
                columnSpecs=AGGREGATION_RULES)
                        
        # write the data
        outstore.append(outkey, aggdf, data_columns=True, min_itemsize=stringLengths)

        outstore.close()
    
    def getGTFSDataFrame(self, period, startIndex=0, route_types=range(0,100)):
        """
        Converts the schedule into a dataframe for the given period
        """
                        
        # create an empty list of dictionaries to store the data
        data = []
        
        # determine the day-of-week as 1=weekday, 2=sat, 3=sun
        dow = getDayOfWeek(period.service_id)
        
        # determine the dates
        dateRange = self.schedule.GetDateRange()
        startDate = pd.to_datetime(dateRange[0], format='%Y%m%d')
        dateRangeString = str(dateRange[0]) + '-' + str(dateRange[1])
        
        # create one record for each trip-stop, specific to the service
        # on this day
        tripList = self.schedule.GetTripList()            
            
        for trip in tripList:
            if trip.service_id == period.service_id:          
                # determine route attributes, and only keep bus trips
                route = self.schedule.GetRoute(trip.route_id)
                if (int(route.route_type) in route_types):
                                            
                    # calculate fare--assume just based on route ID
                    fare = 0
                    fareAttributeList = self.schedule.GetFareAttributeList()
                    for fareAttribute in fareAttributeList:
                        fareRuleList = fareAttribute.GetFareRuleList()
                        for fareRule in fareRuleList:
                            if fareRule.route_id == trip.route_id: 
                                fare = fareAttribute.price
                        
                    # one record for each stop time
                    stopTimeList = trip.GetStopTimes()                            
                        
                    # get shape attributes, converted to a line
                    # this is needed because they are sometimes out of order
                    shapeLine = self.getShapeLine(trip.shape_id, stopTimeList)
                                                
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
                        record['DOW']  = dow
                        record['TOD']  = timeOfDay
                            
                        # observations
                        record['TRIP_STOPS'] = 1
                        record['OBSERVED'] = 0
            
                        # For matching to AVL data
                        record['AGENCY_ID']        = str(route.agency_id).strip().upper()
                        record['ROUTE_SHORT_NAME'] = str(route.route_short_name).strip().upper()
                        record['ROUTE_LONG_NAME']  = str(route.route_long_name).strip().upper()
                        record['DIR']              = str(trip.direction_id).strip().upper()
                        record['TRIP']             = str(firstDeparture) + '_' + str(firstSeq)    # contains sequence and contains HHMM of departure from first stop
                        record['SEQ']              = int(stopTime.stop_sequence)                            
                            
                        # route/trip attributes
                        record['ROUTE_TYPE']       = int(route.route_type)
                        record['TRIP_HEADSIGN']    = str(trip.trip_headsign).strip().upper()
                        record['HEADWAY_S']        = np.NaN             # calculated below
                        record['FARE']             = float(fare)  
                        
                        # stop attriutes
                        record['STOPNAME']         = str(stopTime.stop.stop_name).strip().upper()
                        record['STOP_LAT']         = float(stopTime.stop.stop_lat)
                        record['STOP_LON']         = float(stopTime.stop.stop_lon)
                        record['SOL']              = startOfLine
                        record['EOL']              = endOfLine
                            
                        # stop times        
                        # deal with wrap-around aspect of time (past midnight >2400)
                        arrivalTime = getWrapAroundTime(str(startDate.date()), stopTime.arrival_time)
                        departureTime = getWrapAroundTime(str(startDate.date()), stopTime.departure_time)
                        if startOfLine or endOfLine: 
                            dwellTime = 0.0
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
                                                    
                        # indicates range this schedule is in operation    
                        record['SCHED_DATES'] = dateRangeString          # start and end date for this schedule
                        
                        # track from previous record
                        lastDepartureTime = departureTime      
                        lastDistanceTraveled = distanceTraveled       
                        
                        # gtfs IDs
                        record['ROUTE_ID']  = str(trip.route_id).strip().upper()
                        record['TRIP_ID']   = str(trip.trip_id).strip().upper()
                        record['STOP_ID']   = str(stopTime.stop_id).strip().upper()
                        record['SERVICE_ID']= str(trip.service_id).strip().upper()
                                                                                                                                        
                        data.append(record)                
                        i += 1
                                    
        # convert to data frame 
        print ("service_id %s has %i trip-stop records" % (period.service_id, len(data)))
        df = pd.DataFrame(data)    

        # calculate the headways, based on difference in previous bus on 
        # this route stopping at the same stop
        groupby = ['AGENCY_ID','ROUTE_ID','DIR','TRIP_HEADSIGN','SEQ']
        df = df.groupby(groupby, as_index=False).apply(calculateHeadways)
        
        # sorted
        df.sort(['AGENCY_ID','ROUTE_ID','DIR','TRIP_HEADSIGN','TRIP','SEQ'], inplace=True)    
        df.index = pd.Series(range(startIndex,startIndex+len(df))) 
        
        return df
        
    
    def getShapeLine(self, shape_id, stopTimeList):
        """
        Accepts a shape_id and a list of stop times in GTFS transit feed format.
        If not shape is provided, then just project straight lines between
        each stop.  
        
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
        
        # get the shape
        # if it doesn't exist, return a straight projection between the stops
        try:         
            shape = self.schedule.GetShape(shape_id)
        except KeyError: 
            return stopLine            
        
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
        
        
        