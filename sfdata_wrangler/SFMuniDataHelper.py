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
import os
              
                                    
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
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
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
                         chunksize= self.CHUNKSIZE)

        # establish the writer
        store = pd.HDFStore(outfile)

        # iterate through chunk by chunk so we don't run out of memory
        rowsRead    = 0
        rowsWritten = 0
        for chunk in reader:   

            rowsRead    += len(chunk)
            
            # sometimes the rear-door boardings is 4 digits, in which case 
            # the remaining columns get mis-alinged
            chunk = chunk[chunk['RDBRDNGS']<1000]
            
            # because of misalinged row, it sometimes auto-detects inconsistent
            # data types, so force them as specified.  Must be in same order 
            # as above
            for i in range(0, len(colnames)):
                if (colnames[i] in chunk):
                    if (coltypes[i]=='object'):
                        chunk[colnames[i]] = chunk[colnames[i]].astype('str')
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
                    print 'ROUTE_AVL id ', r, ' not found in route equivalency file'
                
            # convert to timedate formats
            arrTimeInt = pd.Series(zip(chunk['DATE_INT'], chunk['ARRIVAL_TIME_INT']), index=chunk.index)   
            depTimeInt = pd.Series(zip(chunk['DATE_INT'], chunk['DEPARTURE_TIME_INT']), index=chunk.index) 
            pulloutInt = pd.Series(zip(chunk['DATE_INT'], chunk['PULLOUT_INT']), index=chunk.index)   
            
            chunk['DATE']           = chunk['DATE_INT'].apply(self.getDate)    
            chunk['ARRIVAL_TIME']   = arrTimeInt.apply(self.getWrapAroundTime)  
            chunk['DEPARTURE_TIME'] = depTimeInt.apply(self.getWrapAroundTime)      
            chunk['PULLOUT']        = pulloutInt.apply(self.getWrapAroundTime)  
          
                                                                                
            # drop duplicates (not sure why these occur) and sort
            chunk.drop_duplicates(cols=self.INDEX_COLUMNS, inplace=True) 
            chunk.sort(self.INDEX_COLUMNS, inplace=True)
            
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
                print 'Structure of HDF5 file is: '
                print store.sample.dtypes
                store.close()
                print 'Structure of current dataframe is: '
                print df.dtypes
                raise  
            except TypeError: 
                print 'Structure of current dataframe is: '
                types = df.dtypes
                for type in types:
                    print type
                raise

            rowsWritten += len(df)
            print datetime.datetime.now(), ' Read %i rows and kept %i rows.' % (rowsRead, rowsWritten)

        if len(missingRouteIds) > 0: 
            print 'The following AVL route IDs are missing from the routeEquiv file:'
            for missing in missingRouteIds: 
                print '  ', missing
            
        # close the writer
        store.close()


    def aggregateToTrips(self, expanded_file):
        """
        Aggregates the expanded data from trip_stops to trip totals. 
        
        """
                    
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        AGGREGATION_RULES = [              
                ['MONTH'             ,'MONTH'             ,'first'   ,'trip' ,'datetime64', 0],          
                ['SCHED_DATES'       ,'SCHED_DATES'       ,'first'   ,'trip' ,'object'    ,20],      
                ['NUMDAYS'           ,'DATE'     ,self.countUnique   ,'trip' ,'int64'     , 0],         # stats for observations
                ['TRIPS'             ,'TRIPS'             ,'max'     ,'trip' ,'int64'     , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'trip' ,'int64'     , 0], 
                ['OBSERVED'          ,'OBSERVED'          ,'max'     ,'trip' ,'int64'     , 0], 
                ['TRIP_ID'           ,'TRIP_ID'           ,'first'   ,'trip' ,'int64'     , 0],         # trip attributes  
   	        ['SHAPE_ID'          ,'SHAPE_ID'          ,'first'   ,'trip' ,'int64'     , 0],  
       	        ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'trip' ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'trip' ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'trip' ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'trip' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'trip' ,'float64'   , 0],  
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'last'    ,'trip' ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','first'   ,'trip' ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'trip' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'sum'     ,'trip' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'trip' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'sum'     ,'trip' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'trip' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'trip' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'trip' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'trip' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'mean'    ,'trip' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'sum'     ,'trip' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'sum'     ,'trip' ,'float64'   , 0],                           
                ['PASSMILES'         ,'PASSMILES'         ,'sum'     ,'trip' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'sum'     ,'trip' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'sum'     ,'trip' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'sum'     ,'trip' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'sum'     ,'trip' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'sum'     ,'trip' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'sum'     ,'trip' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'sum'     ,'trip' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'sum'     ,'trip' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'sum'     ,'trip' ,'float64'   , 0],
                ['VC'                ,'VC'                ,'max'     ,'trip' ,'float64'   , 0],         # crowding 
                ['CROWDED'           ,'CROWDED'           ,'max'     ,'trip' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'sum'     ,'trip' ,'float64'   , 0]  
                ]
        

        # get all infiles matching the pattern
        pattern = expanded_file.replace('YYYY', '*')
        infiles = glob.glob(pattern)
        print 'Retrieved a total of %i years to process' % len(infiles)
        
        for infile in infiles: 
            
            # open the data store 
            store = pd.HDFStore(infile)            
            
            # get the input 'ts' keys, and delete the output tables
            allKeys = store.keys()
            inkeys = []
            for key in allKeys: 
                if key.startswith('/ts'):
                    inkeys.append(key)
                else: 
                    store.remove(key)                    
            
            print 'Retrieved a total of %i inkeys to process' % len(inkeys)   
    
            # loop through the months, and days of week
            for inkey in inkeys: 
                outkey = inkey.replace('/ts', '/trip')                
                dates = store.select_column(inkey, column='DATE').unique()
                print 'Processing ', inkey, ' to write to ', outkey, ' with ', len(dates), ' days.'
                
                # count the number of rows in each table so our 
                # indices are unique
                trip_count     = 0

                # process for each day
                for date in dates: 

                    df = store.select(inkey, where='DATE=Timestamp(date)')                   
                    
                    # initialize new terms
                    df['TRIPS'] = 1                
                            
                    # routes
                    aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                            groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'TRIP'], 
                            columnSpecs=AGGREGATION_RULES, 
                            level='trip', 
                            weight=None)
                    aggdf.index = trip_count + pd.Series(range(0,len(aggdf)))
                    store.append(outkey, aggdf, data_columns=True, 
                            min_itemsize=stringLengths)   
                    trip_count += len(aggdf)
    
            store.close()


    def aggregateToDays(self, weighted_file, aggregate_file):
        """
        Aggregates weighted data to daily totals.  
            Does this at different levels of aggregation for:
            route-stops, routes, stops, and system, 
        and temporally for:
            time-of-day, daily
        
        """
                    
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        STOP_RULES = [              
                ['MONTH'             ,'MONTH'             ,'first'   ,'system' ,'datetime64', 0],          
                ['SCHED_DATES'       ,'SCHED_DATES'       ,'first'   ,'system' ,'object'    ,20],       
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'system' ,'int64'     , 0],         # stats for observations
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'system' ,'int64'     , 0],         #  note: attributes from schedule/gtfs should be unweighted             
                ['OBS_TRIP_STOPS'    ,'OBSERVED'          ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'TRIP_STOPS'        ,'wgtSum'  ,'system' ,'float64'   , 0], 
   	        ['STOP_ID'           ,'STOP_ID'           ,'first'   ,'route_stop','int64'  , 0],        
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route_stop','object' ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route_stop','int64'  , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route_stop','object' ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'system' ,'float64'   , 0],    
                ['STOPNAME'          ,'STOPNAME'          ,'first'   ,'stop'   ,'object'    ,32],         # stop attributes
                ['STOPNAME_AVL'      ,'STOPNAME_AVL'      ,'first'   ,'stop'   ,'object'    ,32],  
                ['STOP_LAT'          ,'STOP_LAT'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['STOP_LON'          ,'STOP_LON'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['EOL'               ,'EOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['SOL'               ,'SOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['TIMEPOINT'         ,'TIMEPOINT'         ,'first'   ,'stop'   ,'int64'     , 0],     
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'stop'   ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'stop'   ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'wgtSum'  ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'wgtSum'  ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['LOAD_ARR'          ,'LOAD_ARR'          ,'wgtSum'  ,'stop'   ,'float64'   , 0],   
                ['LOAD_DEP'          ,'LOAD_DEP'          ,'wgtSum'  ,'stop'   ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'wgtSum'  ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'wgtSum'  ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'sum'     ,'system' ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'wgtSum'  ,'system' ,'float64'   , 0]  
                ]

   
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        TRIP_RULES = [              
                ['MONTH'             ,'MONTH'             ,'first'   ,'system' ,'datetime64', 0],           
                ['SCHED_DATES'       ,'SCHED_DATES'       ,'first'   ,'system' ,'object'    ,20],          
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'system' ,'int64'     , 0],         # stats for observations
                ['TRIPS'             ,'TRIPS'             ,'sum'     ,'system' ,'int64'     , 0],         #  note: attributes from schedule/gtfs should be unweighted             
                ['OBS_TRIPS'         ,'OBSERVED'          ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'TRIPS'             ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route'  ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route'  ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route'  ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'route'  ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'route'  ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'wgtSum'  ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'wgtSum'  ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'wgtSum'  ,'system' ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'wgtSum'  ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'wgtSum'  ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'wgtSum'  ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],        # crowding
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'wgtSum'  ,'system' ,'float64'   , 0]  
                ]

        # delete the output file if it already exists
        if os.path.isfile(aggregate_file):
            print 'Deleting previous aggregate output'
            os.remove(aggregate_file)                         
        outstore = pd.HDFStore(aggregate_file)
        
        # count the number of rows in each table so our 
        # indices are unique
        rs_tod_count     = 0
        rs_day_count     = 0
        route_tod_count  = 0
        route_day_count  = 0
        stop_tod_count   = 0
        stop_day_count   = 0
        system_tod_count_s = 0
        system_day_count_s = 0
        system_tod_count = 0
        system_day_count = 0


        # get all infiles matching the pattern
        pattern = weighted_file.replace('YYYY', '*')
        infiles = glob.glob(pattern)
        print 'Retrieved a total of %i years to process' % len(infiles)
        
        for infile in infiles: 
            
            # open the data store 
            instore = pd.HDFStore(infile)    
            
            # separate the keys into trip_stop keys and trip keys
            allKeys = instore.keys()
            tripstop_keys = []
            trip_keys = []
            for key in allKeys: 
                if key.startswith('/ts'):
                    tripstop_keys.append(key)
                elif key.startswith('/trip'): 
                    trip_keys.append(key)                   
            
            print 'Retrieved a total of %i trip_stop keys to process' % len(tripstop_keys)   
            for key in tripstop_keys:
                print 'Processing ', key
                
                # get a months worth of data for this day of week
                # be sure we have a clean index
                df = instore.select(key)                        
                df.index = pd.Series(range(0,len(df)))   

                # route_stops    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                        columnSpecs=STOP_RULES, 
                        level='route_stop', 
                        weight='TOD_WEIGHT')      
                aggdf.index = rs_tod_count + pd.Series(range(0,len(aggdf)))
                outstore.append('rs_tod', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)          
                rs_tod_count += len(aggdf)
                                                    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                        columnSpecs=STOP_RULES, 
                        level='route_stop', 
                        weight='DAY_WEIGHT')
                aggdf.index = rs_day_count + pd.Series(range(0,len(aggdf)))
                outstore.append('rs_day', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                rs_day_count += len(aggdf)
    
                # stops
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','TOD','AGENCY_ID','STOP_ID'], 
                        columnSpecs=STOP_RULES, 
                        level='stop', 
                        weight='TOD_WEIGHT')
                aggdf.index = stop_tod_count + pd.Series(range(0,len(aggdf)))
                outstore.append('stop_tod', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                stop_tod_count += len(aggdf)
    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','AGENCY_ID','STOP_ID'], 
                        columnSpecs=STOP_RULES, 
                        level='stop', 
                        weight='DAY_WEIGHT')
                aggdf.index = stop_day_count + pd.Series(range(0,len(aggdf)))
                outstore.append('stop_day', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)  
                stop_day_count += len(aggdf)

                # system
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','TOD','AGENCY_ID'], 
                        columnSpecs=STOP_RULES, 
                        level='system', 
                        weight='SYSTEM_WEIGHT')      
                aggdf.index = system_tod_count_s + pd.Series(range(0,len(aggdf))) 
                outstore.append('system_tod_s', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                system_tod_count_s += len(aggdf)
    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','AGENCY_ID'], 
                        columnSpecs=STOP_RULES, 
                        level='system', 
                        weight='SYSTEM_WEIGHT')     
                aggdf.index = system_day_count_s + pd.Series(range(0,len(aggdf)))                       
                outstore.append('system_day_s', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                system_day_count_s += len(aggdf)
            
            print 'Retrieved a total of %i trip keys to process' % len(trip_keys)   
            for key in trip_keys:
                print 'Processing ', key
                
                # get a months worth of data for this day of week
                # be sure we have a clean index
                df = instore.select(key)                        
                df.index = pd.Series(range(0,len(df)))   
                    
                # routes
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                        columnSpecs=TRIP_RULES, 
                        level='route', 
                        weight='TOD_WEIGHT')
                aggdf.index = route_tod_count + pd.Series(range(0,len(aggdf)))
                outstore.append('route_tod', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                route_tod_count += len(aggdf)
    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                        columnSpecs=TRIP_RULES, 
                        level='route', 
                        weight='DAY_WEIGHT')
                aggdf.index = route_day_count + pd.Series(range(0,len(aggdf)))
                outstore.append('route_day', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)  
                route_day_count += len(aggdf) 
    
                # system
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','TOD','AGENCY_ID'], 
                        columnSpecs=TRIP_RULES, 
                        level='system', 
                        weight='SYSTEM_WEIGHT')      
                aggdf.index = system_tod_count + pd.Series(range(0,len(aggdf))) 
                outstore.append('system_tod', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                system_tod_count += len(aggdf)
    
                aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                        groupby=['DATE','DOW','AGENCY_ID'], 
                        columnSpecs=TRIP_RULES, 
                        level='system', 
                        weight='SYSTEM_WEIGHT')     
                aggdf.index = system_day_count + pd.Series(range(0,len(aggdf)))                       
                outstore.append('system_day', aggdf, data_columns=True, 
                        min_itemsize=stringLengths)   
                system_day_count += len(aggdf)
            
            instore.close()
        outstore.close()

    
    def aggregateToMonths(self, daily_file, monthly_file):
        """
        Aggregates daily data to monthly totals for an average weekday/
        saturday/sunday.  Does this at different levels of aggregation for:
            route-stops, routes, stops, and system, 
        and temporally for:
            trips, time-of-day, day-of-week
        
        These are unweighted, because we've already applied weights when
        calculating the daily totals. 
        """
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        STOP_RULES = [              
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'         ,'OBS_TRIP_STOPS',np.count_nonzero,'system' ,'int64'     , 0],        
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'mean'    ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBSERVED'          ,'mean'    ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'TRIP_STOPS'        ,'mean'    ,'system' ,'float64'   , 0], 
   	        ['STOP_ID'           ,'STOP_ID'           ,'first'   ,'route_stop','int64'  , 0],        
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route_stop','object' ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route_stop','int64'  , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route_stop','object' ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'system' ,'float64'   , 0],    
                ['STOPNAME'          ,'STOPNAME'          ,'first'   ,'stop'   ,'object'    ,32],         # stop attributes
                ['STOPNAME_AVL'      ,'STOPNAME_AVL'      ,'first'   ,'stop'   ,'object'    ,32],  
                ['STOP_LAT'          ,'STOP_LAT'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['STOP_LON'          ,'STOP_LON'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['EOL'               ,'EOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['SOL'               ,'SOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['TIMEPOINT'         ,'TIMEPOINT'         ,'first'   ,'stop'   ,'int64'     , 0],     
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean'    ,'stop'   ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean'    ,'stop'   ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'mean'    ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'mean'    ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'mean'    ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'mean'    ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'mean'    ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'mean'    ,'system' ,'float64'   , 0],   
                ['LOAD_ARR'          ,'LOAD_ARR'          ,'mean'    ,'stop'   ,'float64'   , 0],   
                ['LOAD_DEP'          ,'LOAD_DEP'          ,'mean'    ,'stop'   ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'mean'    ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'mean'    ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'mean'    ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'mean'    ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'mean'    ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'mean'    ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'mean'    ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'mean'    ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'mean'    ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'mean'    ,'system' ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'mean'    ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'mean'    ,'system' ,'float64'   , 0]  
                ]

   
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        TRIP_RULES = [              
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'         ,'OBS_TRIP_STOPS',np.count_nonzero,'system' ,'int64'     , 0],                        
                ['TRIPS'             ,'TRIPS'             ,'mean'    ,'system' ,'int64'     , 0],         #  note: attributes from schedule/gtfs should be unweighted             
                ['OBS_TRIPS'         ,'OBSERVED'          ,'mean'    ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'TRIPS'             ,'mean'    ,'system' ,'float64'   , 0],  
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route'  ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route'  ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route'  ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean'    ,'route'  ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean'    ,'route'  ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'mean'    ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'mean'    ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'mean'    ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'mean'    ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'mean'    ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'mean'    ,'system' ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'mean'    ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'mean'    ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'mean'    ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'mean'    ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'mean'    ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'mean'    ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'mean'    ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'mean'    ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'mean'    ,'system' ,'float64'   , 0],   
                ['VC'                ,'VC'                ,'mean'    ,'system' ,'float64'   , 0],        # crowding
                ['CROWDED'           ,'CROWDED'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'mean'    ,'system' ,'float64'   , 0]  
                ]


        # delete the output file if it already exists
        if os.path.isfile(monthly_file):
            print 'Deleting previous aggregate output'
            os.remove(monthly_file)                         
        outstore = pd.HDFStore(monthly_file)
        
        # count the number of rows in each table so our 
        # indices are unique
        rs_tod_count     = 0
        rs_day_count     = 0
        route_tod_count  = 0
        route_day_count  = 0
        stop_tod_count   = 0
        stop_day_count   = 0
        system_tod_count_s = 0
        system_day_count_s = 0
        system_tod_count = 0
        system_day_count = 0

                
        # open the output file
        instore = pd.HDFStore(daily_file)                      
    
        # route_stops
    
        print 'Processing route_stops by tod'                
        df = instore.select('rs_tod')                        
        df.index = pd.Series(range(0,len(df)))                   
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                columnSpecs=STOP_RULES, 
                level='route_stop', 
                weight=None)      
        aggdf.index = rs_tod_count + pd.Series(range(0,len(aggdf)))

        outstore.append('rs_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)          
        rs_tod_count += len(aggdf)

                                                
        print 'Processing daily route_stops'                
        df = instore.select('rs_day')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                columnSpecs=STOP_RULES, 
                level='route_stop', 
                weight=None)      
        aggdf.index = rs_day_count + pd.Series(range(0,len(aggdf)))

        outstore.append('rs_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        rs_day_count += len(aggdf)                    

        # stops
        print 'Processing stops by tod'                
        df = instore.select('stop_tod')                        
        df.index = pd.Series(range(0,len(df)))                     
            
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID','STOP_ID'], 
                columnSpecs=STOP_RULES, 
                level='stop', 
                weight=None)      
        aggdf.index = stop_tod_count + pd.Series(range(0,len(aggdf)))

        outstore.append('stop_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        stop_tod_count += len(aggdf)    


        print 'Processing daily stops'                
        df = instore.select('stop_day')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID','STOP_ID'], 
                columnSpecs=STOP_RULES, 
                level='stop', 
                weight=None)      
        aggdf.index = stop_day_count + pd.Series(range(0,len(aggdf)))

        outstore.append('stop_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        stop_day_count += len(aggdf)    

        # system
        print 'Processing system by tod'                
        df = instore.select('system_tod_s')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight=None)           
        aggdf.index = system_tod_count_s + pd.Series(range(0,len(aggdf))) 

        outstore.append('system_tod_s', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        system_tod_count_s += len(aggdf)    


        print 'Processing daily system'                
        df = instore.select('system_day_s')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight=None)        
        aggdf.index = system_day_count_s + pd.Series(range(0,len(aggdf)))  
                     
        outstore.append('system_day_s', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        system_day_count_s += len(aggdf)    


        # routes
        print 'Processing routes by tod'                
        df = instore.select('route_tod')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight=None)      
        aggdf.index = route_tod_count + pd.Series(range(0,len(aggdf)))

        outstore.append('route_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        route_tod_count += len(aggdf)    


        print 'Processing daily routes'                
        df = instore.select('route_day')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight=None)      
        aggdf.index = route_day_count + pd.Series(range(0,len(aggdf)))

        outstore.append('route_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        route_day_count += len(aggdf)     


        # system
        print 'Processing system by tod'                
        df = instore.select('system_tod')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight=None)           
        aggdf.index = system_tod_count + pd.Series(range(0,len(aggdf))) 

        outstore.append('system_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        system_tod_count += len(aggdf)    


        print 'Processing daily system'                
        df = instore.select('system_day')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight=None)        
        aggdf.index = system_day_count + pd.Series(range(0,len(aggdf)))  
                     
        outstore.append('system_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        system_day_count += len(aggdf)            
            
        instore.close()
        outstore.close()

    
    def aggregateTransitRecords(self, df, groupby, columnSpecs, level='system', weight=None):
        """
        Aggregates transit records to the groupings specified.  The counting 
        equipment is only on about 25% of the busses, so we need to average 
        across multiple days (in this case the whole month) to account for 
        all of the trips made on each route.
        
        df - dataframe to aggregate

        groupby - a list of column names to groupby

        columnSpecs - 2-dimensional list of specifications for each column

                      where the format is: 
                      [
                      [outfield, infield, aggregationMethod, type, stringLength], 
                      ...
                      [outfield, infield, aggregationMethod, type, stringLength]
                      ]
                      
                      The sub-fields are:

                          outfield - the name of the output column, must be unique

                          infield  - the name of the column in the existing
                                     database, or 'none' if not present

                          aggregationMethod - the name of the method for 
                                     aggregating used by the df.groupby()
                                     method.  Will usually be 'first', 'mean', 
                                     or 'std'.  Can also be 'none' if the field
                                     is not to be aggregated. count will
                                     aggregate the number of records. 

                          maxlevel - the maximum level at which to include this
                                     field.  String should be one of: 
                                         trip, route, system
                                         route_stop, stop

                          type -     the data type for the output field.  Will
                                     usually be 'int64', 'float64', 'object' or
                                     'datetime64'
                          
                          stringLength - if the type is 'object', the width
                                         of the field in characters.  Otherwise 0.
        
        weight - column to use as a weight

        returns - an aggregated dataframe, also the stringLengths to facilitate writing
        """        

        # convert to formats used by standard methods.  
        # Start with the month, which is used for aggregation
        colorder  = list(groupby)
        coltypes  = {}
        stringLengths= {}
        aggMethod = {}        
        countOutFields = set()   
        sumOutFields = set()
        
        wgtSumInFields = set()
        wgtAvgInFields = set()
        wgtAvgOutFields = set()
        
        for col in columnSpecs:
            
            # these are the entries required by the input specification
            outfield    = col[0]
            infield     = col[1]
            aggregation = col[2]
            maxlevel    = col[3]
            dtype       = col[4]
            stringLength= col[5] 
            
            # only include those fields with the appropriate maxlevel
            if level=='system':
                if (maxlevel=='route' or maxlevel=='stop' or maxlevel=='route_stop'):
                    continue
            elif level=='stop':
                if (maxlevel=='route' or maxlevel=='route_stop'):
                    continue                
            elif level=='route':
                if (maxlevel=='stop' or maxlevel=='route_stop'):
                    continue                                
            
            # now populate arrays as needed
            colorder.append(outfield)
            coltypes[outfield] = dtype
            if (dtype=='object'): 
                stringLengths[outfield] = stringLength

            # skip aggregation if none, or no input field
            if aggregation != 'none' and infield != 'none': 
                
                # for keeping track of daily averages
                if aggregation == 'sum' or aggregation == 'wgtSum':
                    sumOutFields.add(outfield)
                
                # for weighted averages
                if aggregation == 'wgtSum':                    
                    wgtSumInFields.add(infield)
                    aggregation = 'sum'
                    infield = 'w' + infield
                elif aggregation == 'wgtAvg': 
                    wgtAvgInFields.add(infield)
                    wgtAvgOutFields.add(outfield)
                    aggregation = 'sum'
                    infield = 'w' + infield
                
                # the main aggregation methods
                if infield in aggMethod: 
                    aggMethod[infield][outfield] = aggregation
                else:
                    aggMethod[infield] = {outfield : aggregation}
                        
            # these fields get the count of the number of records
            if aggregation == 'count': 
                countOutFields.add(outfield)

        # since groupby isn't listed above
        if 'ROUTE_SHORT_NAME' in groupby:
            stringLengths['ROUTE_SHORT_NAME'] = 32

        # include the weight when aggregating
        # scale up any weighted columns  
        if weight != None: 
            aggMethod[weight] = {weight : 'sum'}
            for col in wgtSumInFields.union(wgtAvgInFields):
                df['w'+col] = df[weight] * df[col]
                                                                                                                                 
        # group
        grouped = df.groupby(groupby)
        aggregated = grouped.aggregate(aggMethod)
            
        # drop multi-level columns
        levels = aggregated.columns.levels
        labels = aggregated.columns.labels
        aggregated.columns = levels[1][labels[1]]

        # for any average fields, divide by the sum of the weights
        if weight != None:
            for col in wgtAvgOutFields:
                aggregated[col] = (aggregated[col]).values / (aggregated[weight]).values

        # add count fields
        for field in countOutFields: 
            aggregated[field] = grouped.size()
        
        # update scheduled speed
        speedInput = pd.Series(zip(aggregated['SERVMILES_S'], 
                                   aggregated['RUNTIME_S']), 
                               index=aggregated.index)     
        aggregated['RUNSPEED_S'] = speedInput.apply(self.updateSpeeds)
        
        # update actual speed--based on scheduled service miles for consistency
        speedInput = pd.Series(zip(aggregated['SERVMILES'], 
                                   aggregated['RUNTIME']), 
                               index=aggregated.index)            
        aggregated['RUNSPEED'] = speedInput.apply(self.updateSpeeds)
        
        # force the data types
        # this doesn't work if there are missing values, hence the pass
        for col in coltypes: 
            try: 
                aggregated[col] = aggregated[col].astype(coltypes[col])
            except TypeError:
                pass
            except ValueError: 
                pass
                                                                                        
        # clean up structure of dataframe
        aggregated = aggregated.sort_index()
        aggregated = aggregated.reset_index()     
        aggregated = aggregated[colorder]       

        return aggregated, stringLengths



    def meanTimes(self, datetimeSeries):
        """
        Computes the average of a datetime series. 
        
        datetimeSeries - a series of Datetime objects
        
        returns the average datetime, or last element if all null inputs
                                   
        """

        totSec = 0
        count = 0  # deal with missing values
        for dt in datetimeSeries:
            if pd.notnull(dt):
                days = dt.toordinal()
                totSec += (24*3600*(days) + 3600*(dt.hour) 
                        + 60*(dt.minute) + dt.second) 
                count += 1

        if count==0: 
            return pd.NaT
        else:  
                    
            meanSec = totSec / count
            days = meanSec/(24*3600)
            date = datetime.datetime.fromordinal(days)
            
            stringDatetime = (str(date.month) + ' ' 
                            + str(date.day) + ' ' 
                            + str(date.year) + ' '
                            + str((meanSec/3600)%24) + ' ' 
                            + str((meanSec/60)%60) + ' '
                            + str(meanSec%60))
                   
            mean = pd.to_datetime(stringDatetime, format="%m %d %Y %H %M %S")
    
            return mean
        

    def updateSpeeds(self, speedInputs):
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
        
    def countUnique(self, series):
        """
        Counts the number of unique dates in the group
                                           
        """
        return len(series.unique())       
        
    def getWrapAroundTime(self, (dateInt, timeInt)):
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
            print 'Count not convert ', datetimeString
            raise
            
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