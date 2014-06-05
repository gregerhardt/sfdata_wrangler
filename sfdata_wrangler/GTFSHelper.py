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
        tripList = tripList[0:10]
        for trip in tripList:
            
            route = schedule.GetRoute(trip.route_id)
            stopTimeList = trip.GetStopTimes()
            
            for stopTime in stopTimeList:
                record = {}
                
                # calendar attributes
                record['START_DATE'] = pd.to_datetime(startDate)
                record['END_DATE']   = pd.to_datetime(endDate) 
                record['DOW']        = int(trip.service_id)
                #record['TOD']        
                
                # GTFS index attributes
                record['ROUTE_ID']     = int(trip.route_id)
                record['DIRECTION_ID'] = int(trip.direction_id)
                record['TRIP_ID']      = int(trip.trip_id)
                record['STOP_SEQUENCE']= int(stopTime.stop_sequence)
                record['STOP_ID']      = int(stopTime.stop.stop_id)
                
                # AVL/APC index attributes
                #record['ROUTE']         
                #record['PATTCODE']      
                #record['DIR']           
                #record['TRIP']          
                #record['SEQ']           
                #record['QSTOP']         
                
                # route/trip attributes
                record['AGENCY_ID']        = str(route.agency_id)
                record['BLOCK_ID']         = int(trip.block_id)
                record['SHAPE_ID']         = int(trip.shape_id)
                record['ROUTE_SHORT_NAME'] = str(route.route_short_name)
                record['ROUTE_LONG_NAME']  = str(route.route_long_name)
                record['ROUTE_TYPE']       = int(route.route_type)
                record['TRIP_HEADSIGN']    = str(trip.trip_headsign)
                #record['FARE']              
                
                # stop attriutes
                record['STOP_NAME']        = str(stopTime.stop.stop_name)
                record['STOP_LAT']         = float(stopTime.stop.stop_lat)
                record['STOP_LON']         = float(stopTime.stop.stop_lon)
                #record['ARRIVAL_TIME']     = pd.to_datetime(stopTime.arrival_time)
                #record['DEPARTURE_TIME']   = pd.to_datetime(stopTime.departure_time)
                
                data.append(record)
                                
        # convert to data frame
        print "  adding %i trip-stop records" % len(data)
        
        #df, stringLengths = self.createEmptyDataFrame(data)
        df = pd.DataFrame(data)
        print " length of dataframe is %i" % len(df)
        print df.head()
        print df.dtypes

        # establish the writer
        store = pd.HDFStore(outfile)

        # write the data
        try: 
            store.append('gtfs', df, data_columns=True)
        except ValueError: 
            store = pd.HDFStore(outfile)
            print 'Structure of HDF5 file is: '
            print store.gtfs.dtypes
            store.close()
                
            print 'Structure of current dataframe is: '
            print df.dtypes
                
            raise

        store.close()

