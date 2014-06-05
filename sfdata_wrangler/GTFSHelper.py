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
            
class GTFSHelper():
    """ 
    Methods used for loading and converting General Transit Feed Specification
    (GTFS) files, and relating them to AVL/APC data. 
    
    """

    def createEmptyDataFrame(self): 
        """
        Creates an empty dataframe in the right format for storing GTFS data.
        
        returns dataframe, dictionary with string lengths
        """
        
        # specifies columns in the output dataframe
        #   columnName,        dataType, stringLength
        columnSpecs = [
            # calendar attributes
	   ['START_DATE',     'datetime64', 0],    # date range when schedule applies (GTFS)
	   ['END_DATE',       'datetime64', 0],    # date range when schedule applies (GTFS)
	   ['DOW',            'int64',      0],    # 1-weekday, 2-saturday, 3-sunday (GTFS)
	   ['TOD',            'int64',      0],    # time-of-day calculated from trip departure
	   
	    # GTFS index attributes
	   ['ROUTE_ID',       'int64',   0],    # Route ID (GTFS)
	   ['DIRECTION_ID',   'int64',   0],    # Direction ID (GTFS)
	   ['TRIP_ID',        'int64',   0],    # Trip ID (GTFS)
	   ['STOP_SEQUENCE',  'int64',   0],    # sequence of stops (GTFS)
	   ['STOP_ID',        'int64',   0],    # stop ID (GTFS)
	   
	    # AVL/APC index attributes
	   ['ROUTE',          'int64',   0],    # Route ID (AVL/APC)
	   ['PATTCODE',       'object', 10],    # Pattern code (AVL/APC)
	   ['DIR',            'int64',   0],    # Direction ID (AVL/APC)
	   ['TRIP',           'int64',   0],    # Trip ID (AVL/APC)
	   ['SEQ',            'int64',   0],    # sequence of stops (AVL/APC)
	   ['QSTOP',          'int64',   0],    # stop ID (AVL/APC)

            # route/trip attributes
	   ['AGENCY_ID',          'object', 10],    # Agency ID (GTFS)
	   ['BLOCK_ID',           'int64',   0],    # block ID (GTFS)
	   ['SHAPE_ID',           'int64',   0],    # shape ID (GTFS)
	   ['ROUTE_SHORT_NAME',   'object', 32],    # short route name (GTFS)
	   ['ROUTE_LONG_NAME',    'object', 32],    # long route name (GTFS)
	   ['ROUTE_TYPE',         'int64',   0],    # 0-tram, 3-bus, 5-cable car (GTFS)
	   ['TRIP_HEADSIGN',      'object', 32],    # headsign (GTFS)
	   ['FARE',               'float64', 0],    # fare, USD (GTFS)

            # stop attriutes
	   ['STOP_NAME',          'object',   32],  # stop name (GTFS)
	   ['STOP_LAT',           'float64',   0],  # stop latitude (GTFS)
	   ['STOP_LON',           'float64',   0],  # stop longitude (GTFS)
	   ['ARRIVAL_TIME',       'datetime64',0],  # scheduled arrival time (GTFS)
	   ['DEPARTURE_TIME',     'datetime64',0]   # scheduled departure time (GTFS)
        ]

        # populate to 1-d arrays for input to methods
        colnames  = []   
        coltypes  = []
        stringLengths= {}
        
        for col in columnSpecs:
            name   = col[0]
            dtype  = col[1]
            length = col[2]
            
            colnames.append(name)
            coltypes.append(length)
            if (dtype=='object'): 
                stringLengths[name] = length
                
        # now create the actual dataframe
        df = pd.DataFrame(columns=colnames)
        
        return df, stringLengths

                    
    def processRawData(self, infile, outfile):
        """
        Read GTFS, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in GTFS format
        outfile - output file name in h5 format, same as AVL/APC format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # create an empty list of dictionaries to store the data
        data = []
        
        # establish the feed
        tfl = transitfeed.Loader(feed_path=infile)
        schedule = tfl.Load()

        # determine the dates
        dateRange = schedule.GetDateRange()
        startDate = dateRange[0]
        endDate = dateRange[1]
        
        # create one record for each trip-stop
        tripList = schedule.GetTripList()
        for trip in tripList:
            
            route = schedule.GetRoute(trip.route_id)
            stopTimeList = trip.GetStopTimes()
            
            for stopTime in stopTimeList:
                record = {}
                
                # calendar attributes
                record['START_DATE'] = pd.to_datetime(startDate)
                record['END_DATE']   = pd.to_datetime(endDate) 
                record['DOW']        = trip.service_id
                #record['TOD']        
                
                # GTFS index attributes
                record['ROUTE_ID']     = trip.route_id  
                record['DIRECTION_ID'] = trip.direction_id
                record['TRIP_ID']      = trip.trip_id
                record['STOP_SEQUENCE']= stopTime.stop_sequence
                record['STOP_ID']      = stopTime.stop.stop_id   
                
                # AVL/APC index attributes
                #record['ROUTE']         
                #record['PATTCODE']      
                #record['DIR']           
                #record['TRIP']          
                #record['SEQ']           
                #record['QSTOP']         
                
                # route/trip attributes
                record['AGENCY_ID']        = route.agency_id    
                record['BLOCK_ID']         = trip.block_id
                record['SHAPE_ID']         = trip.shape_id
                record['ROUTE_SHORT_NAME'] = route.route_short_name
                record['ROUTE_LONG_NAME']  = route.route_long_name
                record['ROUTE_TYPE']       = route.route_type
                record['TRIP_HEADSIGN']    = trip.trip_headsign
                #record['FARE']              
                
                # stop attriutes
                record['STOP_NAME']        = stopTime.stop.stop_name
                record['STOP_LAT']         = stopTime.stop.stop_lat
                record['STOP_LON']         = stopTime.stop.stop_lon
                record['ARRIVAL_TIME']     = stopTime.arrival_time
                record['DEPARTURE_TIME']   = stopTime.departure_time
                
                data.append(record)
                                
        # convert to data frame
        print "  adding %i trip-stop records" % len(data)
        print data[1:5]
        
        df, stringLengths = self.createEmptyDataFrame()
        df.append(data, ignore_index=True)
        print " length of dataframe is %i" % len(df)
        print df.head()

        # establish the writer
        store = pd.HDFStore(outfile)

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

