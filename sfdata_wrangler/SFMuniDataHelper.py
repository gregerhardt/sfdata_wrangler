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

              
                                    
class SFMuniDataHelper():
    """ 
    Methods used to read SFMuni Automated Passenger Count (APC) and 
    Automated Vehicle Location (AVL) data into a Pandas data frame.  This
    includes definitions of the variables from the raw data, calculating
    computed fields, and some basic clean-up/quality control. 

    Logic is adapted from a .SPS file provided by SFMTA. 
    
    A note on times/dates:  MUNI considers the day to start and end at 3 am
    for operational purposes.  Therefore times > 2400, are still grouped
    with the day before, but can be considered to happen after midnight. 
    """
    
    # number of rows at top of file to skip
    HEADERROWS = 2
    
    # number of rows to read at a time
    #   This affects runtime.  Tests show:
    #      CHUNKSIZE = 100000: runs for 35 min and still doesn't read 100,000 rows
    #      CHUNKSIZE =  10000: reads 100,000 rows in 10 minutes
    #      CHUNKSIZE =   1000: reads 100,000 rows in 10 minutes
    CHUNKSIZE = 10000

    # by default, read the first 62 columns, through PULLOUT_INT
    COLUMNS_TO_READ = [i for i in range(62)]

    # specifies how to read in each column from raw input files
    #   columnName,        inputColumns, dataType, stringLength
    COLUMNS = [
        ['SEQ',            (  0,   5),   'int64',   0],    # stop sequence
	['V2',             (  6,  10),   'int64',   0],    # not used
	['STOP_AVL',       ( 10,  14),   'int64',   0],    # unique stop no	
	['STOPNAME_AVL',   ( 15,  47),   'object', 32],    # stop name
	['ARRIVAL_TIME_INT',   ( 48,  54),   'int64',   0],    # arrival time
	['ON',             ( 55,  58),   'int64',   0],    # on 
	['OFF',            ( 59,  62),   'int64',   0],    # off
	['LOAD_DEP',       ( 63,  66),   'int64',   0],    # departing load
	['LOADCODE',       ( 67,  67),   'object',  1],    # ADJ=*, BAL=B
	['DATE_INT',       ( 68,  74),   'int64',   0],    # date
	['ROUTE_AVL',      ( 75,  79),   'int64',   0],   
	['PATTERN',        ( 80,  86),   'object',  6],    # schedule pattern
	['BLOCK',          ( 87,  93),   'int64',   0],    
	['LAT',            ( 94, 102),   'float64', 0],    # latitude
	['LON',            (103, 112),   'float64', 0],    # longitude 
	['MILES',          (113, 118),   'float64', 0],    # odometer reading (miles)
	['TRIP',           (119, 123),   'int64',   0],    # trip
	['DOORCYCLES',     (124, 125),   'int64',   0],    # door cycles
	['DELTA',          (126, 130),   'int64',   0],    # delta
	['DOW',            (131, 132),   'int64',   0],    # day of week schedule operated: 1-weekday, 2-saturday, 3-sunday
	['DIR',            (133, 134),   'int64',   0],   
	['SERVMILES',       (135, 140),   'float64', 0],    # delta vehicle miles  - miles bus travels from last stop
	['DLPMIN',         (141, 145),   'float64', 0],    # delta minutes
	['PASSMILES',      (146, 153),   'float64', 0],    # delta passenger miles
	['PASSHOURS',      (154, 160),   'float64', 0],    # delta passenger minutes
	['VEHNO',          (161, 165),   'int64',   0],    # bus number
	['LINE',           (166, 170),   'int64',   0],    # route (APC numeric code)
	['DBNN',           (171, 175),   'int64',   0],    # data batch
	['ARRIVAL_TIME_S_INT', (176, 180),   'int64',   0],    # schedule time
	['RUNTIME_S',      (181, 186),   'float64', 0],    # schedule run time, in decimal minutes
	['RUNTIME',        (187, 192),   'float64', 0],    # runtime from the last schedule point--ARRIVAL_TIME - DEPARTURE_TIME of previous time point. (Excludes DWELL at the time points.), in decimal minutes
	['ODOM',           (193, 198),   'float64', 0],    # not used
	['GODOM',          (199, 204),   'float64', 0],    # distance (GPS)
	['ARRIVAL_TIME_DEV',   (205, 211),   'float64', 0],    # schedule deviation
	['DWELL',          (212, 217),   'float64', 0],    # dwell time interval (decimal minutes) -- (DEPARTURE_TIME - ARRIVAL_TIME)
	['MSFILE',         (218, 226),   'object',  8],    # sign up YYMM
	['QC101',          (227, 230),   'int64',   0],    # not used
	['QC104',          (231, 234),   'int64',   0],    # GPS QC
	['QC201',          (235, 238),   'int64',   0],    # count QC
	['AQC',            (239, 242),   'int64',   0],    # assignment QC
	['RECORD',         (243, 244),   'object',  2],    # record type
	['WHEELCHAIR',     (245, 246),   'int64',   0],    # wheelchair
	['BIKERACK',       (247, 248),   'int64',   0],    # bike rack
	['SP2',            (249, 250),   'int64',   0],    # not used
	['V51',            (251, 257),   'int64',   0],    # not used
	['VERSN',          (258, 263),   'int64',   0],    # import version
	['DEPARTURE_TIME_INT',  (264, 270),   'int64',   0],    # departure time
	['UON',            (271, 274),   'int64',   0],    # unadjusted on
	['UOFF',           (275, 278),   'int64',   0],    # unadjusted off
	['CAPACITY',       (279, 283),   'int64',   0],    # capacity
	['OVER',           (284, 288),   'int64',   0],    # 5 over cap
	['NS',             (289, 290),   'object',  0],    # north/south
	['EW',             (291, 292),   'object',  0],    # east/west
	['MAXVEL',         (293, 296),   'float64', 0],    # max velocity on previous link
	['RDBRDNGS',       (297, 301),   'int64',   0],    # rear door boardings
	['DV',             (302, 304),   'int64',   0],    # division
	['PATTCODE',       (305, 315),   'object', 10],    # pattern code
	['DWDI',           (316, 320),   'float64', 0],    # distance traveled durign dwell
	['RUN',            (321, 328),   'int64',   0],    # run
	['SCHOOL',         (329, 335),   'object',  6],    # school trip
	['TRIPID_2',       (336, 344),   'int64',   0],    # long trip ID
	['PULLOUT_INT',    (345, 351),   'int64',   0],    # movement time
	['DEPARTURE_TIME_S_INT',(352, 356),   'int64',   0],    # scheduled departure time
	['DEPARTURE_TIME_DEV',  (357, 363),   'float64', 0],    # schedule deviation
	['DWELL_S',        (364, 368),   'int64',   0],    # scheduled dwell time
	['RECOVERY_S',     (369, 374),   'float64', 0],    # scheduled EOL recovery
	['RECOVERY',       (375, 380),   'float64', 0],    
	['POLITICAL',      (381, 390),   'int64',   0],    # not used
	['DELTAA',         (391, 397),   'int64',   0],    # distance from stop at arrival
	['DELTAD',         (398, 404),   'int64',   0],    # distance from stop at departure
	['ECNT',           (405, 409),   'int64',   0],    # error count
	['MC',             (410, 412),   'int64',   0],    # municipal code
	['DIV',            (413, 416),   'int64',   0],    # division
	['LASTTRIP',       (417, 421),   'int64',   0],    # previous trip
	['NEXTTRIP',       (422, 426),   'int64',   0],    # next trip
	['V86',            (427, 430),   'int64',   0],    # not used
	['TRIPID_3',       (431, 441),   'int64',   0],   
	['WCC',            (442, 445),   'int64',   0],   
	['BRC',            (446, 449),   'int64',   0],   
	['DWELLI',         (450, 455),   'int64',   0],   
	['QC202',          (456, 459),   'int64',   0],   
	['QC302',          (460, 463),   'int64',   0],   
	['QC303',          (464, 467),   'int64',   0],   
	['QC206',          (468, 471),   'int64',   0],   
	['QC207',          (472, 475),   'int64',   0],   
	['DGFT',           (476, 481),   'int64',   0],   
	['DGM',            (482, 485),   'int64',   0],   
	['DGH',            (486, 489),   'int64',   0],   
	['LRSE',           (490, 494),   'int64',   0],   
	['LRFT',           (495, 499),   'int64',   0],   
	['ARRIVEP',        (500, 507),   'int64',   0],   
	['DEPARTP',        (508, 515),   'int64',   0],   
	['DWELLP',         (516, 522),   'int64',   0],   
	['NRSE',           (523, 527),   'int64',   0],   
	['NRFT',           (528, 533),   'int64',   0],   
	['SC',             (534, 536),   'int64',   0],   
	['T_MILE',         (537, 543),   'int64',   0],   
	['CARS',           (544, 547),   'int64',   0]
        ] 

    # set the order of the columns in the resulting dataframe
    REORDERED_COLUMNS=[  
                # calendar attributes
		'DATE'      ,   # ( 68,  74) - date
                'DOW'       ,   #            - day of week schedule operated: 1-weekday, 2-saturday, 3-sunday
		
		# index attributes
		'ROUTE_AVL' ,   # ( 75,  79)
		'AGENCY_ID' ,       #        - matches to GTFS data
		'ROUTE_SHORT_NAME', #        - matches to GTFS data
		'ROUTE_LONG_NAME',  #        - matches to GTFS data
		'DIR'       ,   #            - direction, 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
		'TRIP'      ,   # (119, 123) - trip 
                'SEQ'       ,   # (  0,   5) - stop sequence
                                
                # route/trip attributes
		'PATTCODE'  ,   # (305, 315) - pattern code
		'VEHNO'     ,   # (161, 165) - bus number
		'SCHOOL'    ,   # (329, 335) - school trip
		
		# stop attributes
		'STOP_AVL'  ,   # ( 10,  14) - unique stop no	
		'STOPNAME_AVL', # ( 15,  47) - stop name	
		'TIMEPOINT' ,   #            - flag indicating a schedule time point
		
		# location information
		'LAT'       ,   # ( 94, 102) - latitude
		'LON'       ,   # (103, 112) - longitude 
		'SERVMILES'  ,   # (135, 140) - delta vehicle miles - miles bus travels from last stop

                # ridership
		'ON'        ,   # ( 55,  58) - on 
		'OFF'       ,   # ( 59,  62) - off
		'LOAD_ARR'  ,   #            - arriving load
		'LOAD_DEP'  ,   # ( 63,  66) - departing load
		'PASSMILES' ,   # (146, 153) - delta passenger miles - LOAD_ARR * SERVMILES
		'PASSHOURS' ,   # (154, 160) - delta passenger hours - LOAD_ARR * DLPMIN / 60 -- NOT SURE THIS IS RIGHT
		'RDBRDNGS'  ,   # (297, 300) - rear door boardings
		'LOADCODE'  ,   # ( 67,  67) - ADJ=*, BAL=B
		'CAPACITY'  ,   # (279, 283) - capacity
		'DOORCYCLES',   # (124, 125) - door cycles
		'WHEELCHAIR',   # (245, 246) - wheelchair
		'BIKERACK'  ,   # (247, 248) - bike rack

                # times
		'ARRIVAL_TIME'  ,   # ( 48,  54) - arrival time
		'DEPARTURE_TIME' ,  # (264, 270) - departure time	
		'DWELL'     ,       # (212, 217) - dwell time (decimal minutes) -- (DEPARTURE_TIME - ARRIVAL_TIME), zero at first and last stop
		'RUNTIME'   ,       # note that the value from the data set looks weird
		'PULLOUT'   ,       # (345, 351) - movement time
		
		]         
		    
    # uniquely define the records
    INDEX_COLUMNS=['DATE', 'ROUTE_AVL', 'DIR', 'TRIP','SEQ'] 


    def __init__(self):
        """
        Constructor.                 
        """        
        self.routeEquiv = {}
        
        
    def readRouteEquiv(self, routeEquivFile): 
        df = pd.read_csv(routeEquivFile, index_col='ROUTE_AVL')
        
        # normalize the strings
        df['AGENCY_ID'] = df['AGENCY_ID'].apply(str.strip)
        df['AGENCY_ID'] = df['AGENCY_ID'].apply(str.upper)
        df['ROUTE_SHORT_NAME'] = df['ROUTE_SHORT_NAME'].apply(str.strip)
        df['ROUTE_SHORT_NAME'] = df['ROUTE_SHORT_NAME'].apply(str.upper)
        df['ROUTE_LONG_NAME'] = df['ROUTE_LONG_NAME'].apply(str.strip)
        df['ROUTE_LONG_NAME'] = df['ROUTE_LONG_NAME'].apply(str.upper)        
        
        self.routeEquiv = df
        
            
    def processRawData(self, infile, outfile):
        """
        Read SFMuniData, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in "raw STP" format
        outfile - output file name in h5 format
        """
        
        print (datetime.datetime.now().ctime(), 'Converting raw data in file: ', infile)
        
        # convert column specs 
        colnames = []       
        colspecs = []
        coltypes = []
        stringLengths= {}
        for col in self.COLUMNS: 
            colnames.append(col[0])
            colspecs.append(col[1])
            coltypes.append(col[2])
            if (col[2]=='object' and col[3]>0 and 
                (col[0] in self.REORDERED_COLUMNS)): 
                stringLengths[col[0]] = col[3]
        stringLengths['AGENCY_ID']        = 10
        stringLengths['ROUTE_SHORT_NAME'] = 10
        stringLengths['ROUTE_LONG_NAME']  = 32

        # for tracking undefined route equivalencies
        missingRouteIds = set()

        # set up the reader
        reader = pd.read_fwf(infile,  
                         names    = colnames, 
                         colspecs = colspecs,
                         skiprows = self.HEADERROWS, 
                         usecols  = self.COLUMNS_TO_READ, 
                         iterator = True, 
                         skip_blank_lines = True, 
                         chunksize= self.CHUNKSIZE, 
                         na_values=['ID'])             # because of headers in middle of file

        # establish the writer
        store = pd.HDFStore(outfile)

        # iterate through chunk by chunk so we don't run out of memory
        rowsRead    = 0
        rowsWritten = 0
        for chunk in reader:   

            rowsRead    += len(chunk)
                       
            # sometimes the header is stuck in the middle of the file.  drop those records
            chunk = chunk.dropna(axis=0, subset=['SEQ'])
            
            # sometimes the rear-door boardings is 4 digits, in which case 
            # the remaining columns get mis-alinged
            chunk['RDBRDNGS'] = chunk['RDBRDNGS'].astype('int64')
            chunk = chunk[chunk['RDBRDNGS']<1000]
                                    
            # because of misalinged row, it sometimes auto-detects inconsistent
            # data types, so force them as specified.  Must be in same order 
            # as above
            for i in range(0, len(colnames)):
                if (colnames[i] in chunk):
                    if (coltypes[i]=='object'):
                        chunk[colnames[i]] = chunk[colnames[i]].astype('str')
                    elif (coltypes[i]=='int64'): 
                        chunk[colnames[i]] = (chunk[colnames[i]].astype('float64')).astype('int64')
                    else: 
                        chunk[colnames[i]] = chunk[colnames[i]].astype(coltypes[i])
                                    
            # only include revenue service
            # dir codes: 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
            chunk = chunk[chunk['DIR'] < 2]
    
            # filter by count QC (<=20 is default)
            chunk = chunk[chunk['QC201'] <= 20]
            
            # filter where there is no route, no stop or not trip identified
            chunk = chunk[chunk['ROUTE_AVL']>0]
            chunk = chunk[chunk['STOP_AVL']<9999]
            chunk = chunk[chunk['TRIP']<9999]
            
            # calculate some basic data adjustments
            chunk['LON']      = -1 * chunk['LON']
            chunk['LOAD_ARR'] = chunk['LOAD_DEP'] - chunk['ON'] + chunk['OFF']
            
            # some calculated rows
            chunk['TIMEPOINT'] = np.where(chunk['ARRIVAL_TIME_S_INT'] < 9999, 1, 0)
            chunk['EOL'] = chunk['STOPNAME_AVL'].apply(lambda x: str(x).count('- EOL'))
            chunk['DWELL'] = np.where(chunk['EOL'] == 1, 0, chunk['DWELL'])
            chunk['DWELL'] = np.where(chunk['SEQ'] == 1, 0, chunk['DWELL'])
            
            # match to GTFS indices using route equivalency
            chunk['AGENCY_ID']        = chunk['ROUTE_AVL'].map(self.routeEquiv['AGENCY_ID'])
            chunk['ROUTE_SHORT_NAME'] = chunk['ROUTE_AVL'].map(self.routeEquiv['ROUTE_SHORT_NAME'])
            chunk['ROUTE_LONG_NAME']  = chunk['ROUTE_AVL'].map(self.routeEquiv['ROUTE_LONG_NAME'])
                        
            # check for missing route IDs
            for r in chunk['ROUTE_AVL'].unique(): 
                if not r in self.routeEquiv.index: 
                    missingRouteIds.add(r)
                    print ('ROUTE_AVL id ', r, ' not found in route equivalency file')
                
            # convert to timedate formats
            chunk['DATE']           = chunk['DATE_INT'].apply(self.getDate)                
            chunk['ARRIVAL_TIME']   = chunk.apply(lambda row: self.getWrapAroundTime(row['DATE_INT'], row['ARRIVAL_TIME_INT']), axis=1) 
            chunk['DEPARTURE_TIME'] = chunk.apply(lambda row: self.getWrapAroundTime(row['DATE_INT'], row['DEPARTURE_TIME_INT']), axis=1) 
            chunk['PULLOUT']        = chunk.apply(lambda row: self.getWrapAroundTime(row['DATE_INT'], row['PULLOUT_INT']), axis=1)                       
                            
            # drop duplicates (not sure why these occur) and sort
            chunk.drop_duplicates(subset=self.INDEX_COLUMNS, inplace=True) 
            chunk.sort_values(self.INDEX_COLUMNS, inplace=True)
                        
            # set a unique index
            chunk.index = rowsWritten + pd.Series(range(0,len(chunk)))
                            
            # re-order the columns
            df = chunk[self.REORDERED_COLUMNS]
        
            # write the data
            try: 
                store.append('sample', df, data_columns=True, 
                    min_itemsize=stringLengths)
            except ValueError: 
                store = pd.HDFStore(outfile)
                print ('Structure of HDF5 file is: ')
                print (store.sample.dtypes)
                store.close()
                print ('Structure of current dataframe is: ')
                print (df.dtypes)
                raise  
            except TypeError: 
                print ('Structure of current dataframe is: ')
                types = df.dtypes
                for type in types:
                    print (type)
                raise
            
            rowsWritten += len(df)
            print(datetime.datetime.now().ctime(), ' Read %i rows and kept %i rows.' % (rowsRead, rowsWritten))

        if len(missingRouteIds) > 0: 
            print ('The following AVL route IDs are missing from the routeEquiv file:')
            for missing in missingRouteIds: 
                print('  ', missing)
            
        # close the writer
        store.close()

        
    def getWrapAroundTime(self, dateInt, timeInt):
        """
        Converts a string in the format '%H%M%S' to a datetime object.
        Accounts for the convention where service after midnight is counted
        with the previous day, so input times can be >24 hours. 
        """        
                
        if timeInt>= 240000:
            timeInt = timeInt - 240000
            nextDay = True
        else: 
            nextDay = False
            
        dateString = "{0:0>6}".format(dateInt)  
        timeString = "{0:0>6}".format(timeInt)      
        datetimeString = dateString + ' ' + timeString    
        
        try: 
            time = pd.to_datetime(datetimeString, format="%m%d%y %H%M%S")
        except ValueError:
            print ('Count not convert ', datetimeString)
            time = pd.NaT
            
        if nextDay: 
            time = time + pd.DateOffset(days=1)
        
        return time
        
    
    def getDate(self, dateInt):
        """
        Converts an integer in the format "%m%d%y" into a datetime object.
        """
        dateString = "{0:0>6}".format(dateInt)          
        date = pd.to_datetime(dateString, format="%m%d%y")
        return date