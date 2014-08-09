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


def calculateWeight(df):
    """
    Calculates a WEIGHT.  This will be used to scale up ridership. 
    The weight accounts for the fact that even after imputating missing values, 
    some trips are still not observed for the month.  Other trips on the same
    route are factored up make up for that.  

    The input dataframe shoudl be gruoped by: 
    ['TOD', 'AGENCY_ID','ROUTE_SHORT_NAME','ROUTE_LONG_NAME','DIR','SEQ']
    (but not by TRIP).     
    
    Also, it should be specific to a month and a DOW. 
    """            
    
    totalValues = 0.0
    for i, row in df.iterrows():    
        if ((df['OBSTRIPS'][i] + df['IMPTRIPS'][i])>0): 
            totalValues += 1.0
    
    numRecords = float(len(df))
    if totalValues>0: 
        weight = numRecords / totalValues
    else:
        weight = np.NaN
        
    df['WEIGHT'] = 0.0
    for i, row in df.iterrows():    
        if ((df['OBSTRIPS'][i] + df['IMPTRIPS'][i])>0): 
            df['WEIGHT'][i] = weight

    return df                                                
                
                                    
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
    CHUNKSIZE = 100000

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
	['LOADCODE',       ( 67,  67),   'int64',   0],    # ADJ=*, BAL=B
	['DATE_INT',       ( 68,  74),   'int64',   0],    # date
	['ROUTE_AVL',      ( 75,  79),   'int64',   0],   
	['PATTERN',        ( 80,  86),   'int64',   0],    # schedule pattern
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
	['MSFILE',         (218, 226),   'int64',   0],    # sign up YYMM
	['QC101',          (227, 230),   'int64',   0],    # not used
	['QC104',          (231, 234),   'int64',   0],    # GPS QC
	['QC201',          (235, 238),   'int64',   0],    # count QC
	['AQC',            (239, 242),   'int64',   0],    # assignment QC
	['RECORD',         (243, 244),   'int64',   0],    # record type
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
	['SCHOOL',         (329, 335),   'int64',   0],    # school trip
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
		'DEPARTURE_TIME' ,   # (264, 270) - departure time	
		'DWELL'     ,   # (212, 217) - dwell time (decimal minutes) -- (DEPARTURE_TIME - ARRIVAL_TIME), zero at first and last stop
		'PULLOUT'   ,   # (345, 351) - movement time
		'PULLDWELL' ,   #            - pullout dwell (time interval between door close and movement), excluding end-of-line
		
		]         
		    
    # uniquely define the records
    INDEX_COLUMNS=['DATE', 'ROUTE_AVL', 'DIR', 'TRIP','SEQ'] 

    def __init__(self):
        """
        Constructor.
         
        routeEquivFile - CSV file containing equivalency between AVL route IDs
                         and GTFS route IDs.                  
        """        
        self.routeEquiv = pd.DataFrame() 
        
        
    def readRouteEquiv(self, routeEquivFile): 
        self.routeEquiv = pd.read_csv(routeEquivFile, index_col='ROUTE_AVL')

                    
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
            if (col[2]=='object' and col[3]>0): 
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
            for i in range(0, len(chunk.columns.names)):
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
            
            # generate empty fields        
            chunk['TIMEPOINT'] = 0 
            chunk['EOL'] = 0
            chunk['AGENCY_ID'] = ''
            chunk['ROUTE_SHORT_NAME'] = ''
            chunk['ROUTE_LONG_NAME'] = ''
            chunk['OBSERVED'] = 1       # all records here are observed   
            
            # iterate through the rows for computed fields
            for i, row in chunk.iterrows():
                
                # identify scheduled time points
                if (chunk['ARRIVAL_TIME_S_INT'][i] < 9999): 
                    chunk['TIMEPOINT'][i] = 1
                
                # identify end-of-line stops
                chunk['EOL'][i] = str(chunk['STOPNAME_AVL'][i]).count("- EOL")            
                
                # exclude beginning and end of line from DWELL time
                if ((chunk['EOL'][i] == 1) or (chunk['SEQ'][i] == 1)): 
                    chunk['DWELL'][i] = 0
                                        
                # match to GTFS indices using route equivalency
                r = chunk['ROUTE_AVL'][i]
                try: 
                    chunk['AGENCY_ID'][i]        = str(self.routeEquiv.loc[r, 'AGENCY_ID'])
                    chunk['ROUTE_SHORT_NAME'][i] = str(self.routeEquiv.loc[r, 'ROUTE_SHORT_NAME'])
                    chunk['ROUTE_LONG_NAME'][i]  = str(self.routeEquiv.loc[r, 'ROUTE_LONG_NAME'])
                except KeyError: 
                    missingRouteIds.add(r)
            
            # convert to timedate formats
            # trick here is that the MUNI service day starts and ends at 3 am, 
            # so boardings from midnight to 3 have a service date of the day before
            chunk['DATE']        = ''
            chunk['ARRIVAL_TIME']    = ''
            chunk['DEPARTURE_TIME']   = ''
            chunk['PULLOUT']     = ''
            chunk['PULLDWELL']   = 0.0
    
            # convert to string formats
            for i, row in chunk.iterrows():        
                chunk['DATE'][i] = "{0:0>6}".format(chunk['DATE_INT'][i])   
                
                if (chunk['ARRIVAL_TIME_INT'][i] >= 240000): 
                    chunk['ARRIVAL_TIME_INT'][i] = chunk['ARRIVAL_TIME_INT'][i] - 240000
                chunk['ARRIVAL_TIME'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['ARRIVAL_TIME_INT'][i]))         
    
                if (chunk['DEPARTURE_TIME_INT'][i] >= 240000): 
                    chunk['DEPARTURE_TIME_INT'][i] = chunk['DEPARTURE_TIME_INT'][i] - 240000
                chunk['DEPARTURE_TIME'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['DEPARTURE_TIME_INT'][i]))
    
                if (chunk['PULLOUT_INT'][i] >= 240000): 
                    chunk['PULLOUT_INT'][i] = chunk['PULLOUT_INT'][i] - 240000
                chunk['PULLOUT'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['PULLOUT_INT'][i]))               
                
            # convert to timedate formats
            chunk['DATE']   = pd.to_datetime(chunk['DATE'], format="%m%d%y")            
            chunk['ARRIVAL_TIME']    = pd.to_datetime(chunk['ARRIVAL_TIME'],    format="%m%d%y%H%M%S")        
            chunk['DEPARTURE_TIME']   = pd.to_datetime(chunk['DEPARTURE_TIME'],   format="%m%d%y%H%M%S")    
            chunk['PULLOUT']     = pd.to_datetime(chunk['PULLOUT'],     format="%m%d%y%H%M%S")
    
            # deal with offsets for midnight to 3 am
            for i, row in chunk.iterrows():       
                if (chunk['ARRIVAL_TIME'][i].hour < 3): 
                    chunk['ARRIVAL_TIME'][i] = chunk['ARRIVAL_TIME'][i] + pd.DateOffset(days=1)
    
                if (chunk['DEPARTURE_TIME'][i].hour < 3): 
                    chunk['DEPARTURE_TIME'][i] = chunk['DEPARTURE_TIME'][i] + pd.DateOffset(days=1)
    
                if (chunk['PULLOUT'][i].hour < 3): 
                    chunk['PULLOUT'][i]   = chunk['PULLOUT'][i] + pd.DateOffset(days=1)
                                            
                # PULLDWELL = pullout dwell (time interval between door close and movement)
                if (chunk['EOL'][i]==0):
                    pulldwell = chunk['PULLOUT'][i] - chunk['DEPARTURE_TIME'][i]
                    chunk['PULLDWELL'][i] = round(pulldwell.seconds / 60.0, 2)
                    
            # drop duplicates (not sure why these occur) and sort
            chunk.drop_duplicates(cols=self.INDEX_COLUMNS, inplace=True) 
            chunk.sort(self.INDEX_COLUMNS, inplace=True)
                            
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
            print 'Read %i rows and kept %i rows.' % (rowsRead, rowsWritten)

        if len(missingRouteIds) > 0: 
            print 'The following AVL route IDs are missing from the routeEquiv file:'
            for missing in missingRouteIds: 
                print '  ', missing
            
        # close the writer
        store.close()
    
    
    
    def aggregateTransitRecords(self, df, columnSpecs):
        """
        Calculates monthly averages.  The counting equipment is only on about
        25% of the busses, so we need to average across multiple days (in this
        case the whole month) to account for all of the trips made on each route.
        
        df - dataframe to aggregate

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
                                     is not to be aggregated, or 'groupby'
                                     if it is one of the groupby fields. count will
                                     aggregate the number of records. 

                          type -     the data type for the output field.  Will
                                     usually be 'int64', 'float64', 'object' or
                                     'datetime64'
                          
                          stringLength - if the type is 'object', the width
                                         of the field in characters.  Otherwise 0.
        
        returns - an aggregated dataframe, also the stringLengths to facilitate writing
        """        

        # convert to formats used by standard methods.  
        # Start with the month, which is used for aggregation
        colorder  = []   
        coltypes  = {}
        stringLengths= {}
        groupby   = []
        aggMethod = {}
        countFields = []
        
        for col in columnSpecs:
            
            # these are the entries required by the input specification
            outfield    = col[0]
            infield     = col[1]
            aggregation = col[2]
            dtype       = col[3]
            stringLength= col[4] 
            
            # now populate arrays as needed
            colorder.append(outfield)
            coltypes[outfield] = dtype
            if (dtype=='object'): 
                stringLengths[outfield] = stringLength

            # skip aggregation if none, or no input field
            if aggregation != 'none' and infield != 'none': 
                if aggregation == 'groupby': 
                    groupby.append(outfield)
                else:     
                    if infield in aggMethod: 
                        aggMethod[infield][outfield] = aggregation
                    else:
                        aggMethod[infield] = {outfield : aggregation}
                        
            # these fields get the count of the number of records
            if aggregation == 'count': 
                countFields.append(outfield)
                                           
        # group
        grouped = df.groupby(groupby)
        aggregated = grouped.aggregate(aggMethod)
            
        # drop multi-level columns
        levels = aggregated.columns.levels
        labels = aggregated.columns.labels
        aggregated.columns = levels[1][labels[1]]

        # add count fields
        for field in countFields: 
            aggregated[field] = grouped.size()
                                            
        # clean up structure of dataframe
        aggregated = aggregated.sort_index()
        aggregated = aggregated.reset_index()     
        aggregated = aggregated[colorder]       

        return aggregated, stringLengths


    def calcMonthlyAverages(self, hdf_infile, hdf_aggfile, inkey, outkey):
        """
        Calculates monthly averages.  The counting equipment is only on about
        25% of the busses, so we need to average across multiple days (in this
        case the whole month) to account for all of the trips made on each route.
        
        hdf_infile - HDF5 file with detailed sample data to aggregate
        hdf_aggfile- HDF5 file for writing monthly averages
        inkey   - key to read data from (i.e. 'sample')
        outkey  - key to write out in HDFstore (i.e. 'avg_daily')
                  This determines both the name of the dataframe written to the
                  HDFStore, and also the days selected for averaging. 
                           
        """        

        # specify 'groupby' as aggregation method as appropriate
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #   outfield,            infield,  aggregationMethod, type, stringLength                
        columnSpecs = [              
            ['MONTH'            ,'MONTH'         ,'first'   ,'datetime64', 0],         # monthly aggregations   
            ['DOW'              ,'DOW'           ,'first'   ,'int64'     , 0],
            ['TOD'              ,'TOD'           ,'first'   ,'object'    ,10],
            ['NUMDAYS'          ,'NUMDAYS'       ,'first'   ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'         ,'none'          ,'count'   ,'int64'     , 0],      
            ['OBSTRIPS'         ,'OBSERVED'      ,'sum'     ,'float64'   , 0],   
            ['IMPTRIPS'         ,'IMPTRIPS'      ,'sum'     ,'float64'   , 0],    
            ['WEIGHT'           ,'WEIGHT'        ,'mean'    ,'float64'   , 0],   
   	    ['AGENCY_ID'        ,'AGENCY_ID'     ,'groupby'  ,'object'   ,10],         # grouping fields        
   	    ['ROUTE_SHORT_NAME' ,'ROUTE_SHORT_NAME','groupby','object'   ,10],         
   	    ['ROUTE_LONG_NAME'  ,'ROUTE_LONG_NAME','groupby' ,'object'   ,32],  
            ['DIR'              ,'DIR'           ,'groupby' ,'int64'     , 0], 
            ['TRIP'             ,'TRIP'          ,'groupby' ,'int64'     , 0], 
            ['SEQ'              ,'SEQ'           ,'groupby' ,'int64'     , 0],       
            ['ROUTE_TYPE'       ,'ROUTE_TYPE'    ,'first'   ,'int64'     , 0],         # route attributes  
            ['TRIP_HEADSIGN'    ,'TRIP_HEADSIGN' ,'first'   ,'object'    ,64],   
            ['HEADWAY'          ,'HEADWAY'       ,'mean'    ,'float64'   , 0],   
            ['FARE'             ,'FARE'          ,'mean'    ,'float64'   , 0], 
            ['PATTCODE'         ,'PATTCODE'      ,'first'   ,'object'    ,10],  
	    ['SCHOOL'           ,'SCHOOL'        ,'first'   ,'int64'     , 0],    
            ['STOPNAME'         ,'STOPNAME'      ,'first'   ,'object'    ,32],         # stop attributes
            ['STOPNAME_AVL'     ,'STOPNAME_AVL'  ,'first'   ,'object'    ,32],  
            ['STOP_LAT'         ,'STOP_LAT'      ,'first'   ,'float64'   , 0],   
            ['STOP_LON'         ,'STOP_LON'      ,'first'   ,'float64'   , 0],   
            ['EOL'              ,'EOL'           ,'first'   ,'int64'     , 0],   
            ['SOL'              ,'SOL'           ,'first'   ,'int64'     , 0],   
            ['TIMEPOINT'        ,'TIMEPOINT'     ,'first'   ,'int64'     , 0],  
            ['ARRIVAL_TIME_S'   ,'ARRIVAL_TIME_S',self.meanTimes  ,'datetime64', 0],   # times   
            ['ARRIVAL_TIME'     ,'ARRIVAL_TIME'  ,self.meanTimes  ,'datetime64', 0],      
            ['ARRIVAL_TIME_DEV' ,'ARRIVAL_TIME_DEV','mean'        ,'float64'   , 0],   
            ['DEPARTURE_TIME_S' ,'DEPARTURE_TIME_S',self.meanTimes,'datetime64', 0],  
            ['DEPARTURE_TIME'   ,'DEPARTURE_TIME'  ,self.meanTimes,'datetime64', 0],  
            ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV' ,'mean'    ,'float64'   , 0],   
            ['DWELL_S'          ,'DWELL_S'       ,'mean'    ,'float64'   , 0],
            ['DWELL'            ,'DWELL'         ,'mean'    ,'float64'   , 0],    
            ['RUNTIME_S'        ,'RUNTIME_S'     ,'mean'    ,'float64'   , 0],
            ['RUNTIME'          ,'RUNTIME'       ,'mean'    ,'float64'   , 0],   
            ['SERVMILES'        ,'SERVMILES'     ,'mean'    ,'float64'   , 0],
            ['SERVMILES_AVL'    ,'SERVMILES_AVL' ,'mean'    ,'float64'   , 0],    
            ['RUNSPEED_S'       ,'RUNSPEED_S'    ,'mean'    ,'float64'   , 0],
            ['RUNSPEED'         ,'RUNSPEED'      ,'mean'    ,'float64'   , 0],                
            ['ONTIME4'          ,'ONTIME4'       ,'mean'    ,'float64'   , 0],   
            ['ONTIME10'         ,'ONTIME10'      ,'mean'    ,'float64'   , 0],              
            ['ON'               ,'ON'            ,'mean'    ,'float64'   , 0],          # ridership   
            ['OFF'              ,'OFF'           ,'mean'    ,'float64'   , 0],  
            ['LOAD_ARR'         ,'LOAD_ARR'      ,'mean'    ,'float64'   , 0],  
            ['LOAD_DEP'         ,'LOAD_DEP'      ,'mean'    ,'float64'   , 0],            
            ['PASSMILES'        ,'PASSMILES'     ,'mean'    ,'float64'   , 0],   
            ['PASSHOURS'        ,'PASSHOURS'     ,'mean'    ,'float64'   , 0],   
            ['WAITHOURS'        ,'WAITHOURS'     ,'mean'    ,'float64'   , 0],   
            ['PASSDELAY_DEP'    ,'PASSDELAY_DEP' ,'mean'    ,'float64'   , 0],   
            ['PASSDELAY_ARR'    ,'PASSDELAY_ARR' ,'mean'    ,'float64'   , 0],   
            ['RDBRDNGS'         ,'RDBRDNGS'      ,'mean'    ,'float64'   , 0],   
            ['CAPACITY'         ,'CAPACITY'      ,'mean'    ,'float64'   , 0],   
            ['DOORCYCLES'       ,'DOORCYCLES'    ,'mean'    ,'float64'   , 0],   
            ['WHEELCHAIR'       ,'WHEELCHAIR'    ,'mean'    ,'float64'   , 0],   
            ['BIKERACK'         ,'BIKERACK'      ,'mean'    ,'float64'   , 0],   
            ['VC'               ,'VC'            ,'mean'    ,'float64'   , 0],        # crowding
            ['CROWDED'          ,'CROWDED'       ,'mean'    ,'float64'   , 0],   
            ['CROWDHOURS'       ,'CROWDHOURS'    ,'mean'    ,'float64'   , 0], 
            #['ARRIVAL_TIME_DEV_STD' ,'ARRIVAL_TIME_DEV','std','float64'  , 0],         # standard deviations    
            #['DEPARTURE_TIME_DEV_STD','DEPARTURE_TIME_DEV','std','float64',0],   
            #['DWELL_STD'        ,'DWELL'         ,'std'     ,'float64'   , 0],    
            #['RUNTIME_STD'      ,'RUNTIME'       ,'std'     ,'float64'   , 0], 
            #['RUNSPEED_STD'     ,'RUNSPEED'      ,'std'     ,'float64'   , 0],     
            #['ON_STD'           ,'ON'            ,'std'     ,'float64'   , 0],  
            #['OFF_STD'          ,'OFF'           ,'std'     ,'float64'   , 0],  
            #['LOAD_ARR_STD'     ,'LOAD_ARR'      ,'std'     ,'float64'   , 0],  
            #['LOAD_DEP_STD'     ,'LOAD_DEP'      ,'std'     ,'float64'   , 0],   
            #['PASSMILES_STD'    ,'PASSMILES'     ,'std'     ,'float64'   , 0],   
            #['PASSHOURS_STD'    ,'PASSHOURS'     ,'std'     ,'float64'   , 0],  
            #['WAITHOURS_STD'    ,'WAITHOURS'     ,'std'     ,'float64'   , 0],
            #['PASSDELAY_DEP_STD','PASSDELAY_DEP' ,'std'     ,'float64'   , 0], 
            #['PASSDELAY_ARR_STD','PASSDELAY_ARR' ,'std'     ,'float64'   , 0], 
            #['RDBRDNGS_STD'     ,'RDBRDNGS'      ,'std'     ,'float64'   , 0],  
            #['DOORCYCLES_STD'   ,'DOORCYCLES'    ,'std'     ,'float64'   , 0],  
            #['WHEELCHAIR_STD'   ,'WHEELCHAIR'    ,'std'     ,'float64'   , 0],  
            #['BIKERACK_STD'     ,'BIKERACK'      ,'std'     ,'float64'   , 0],              
            ['ROUTE_ID'         ,'ROUTE_ID'      ,'first'   ,'int64'     , 0],         # additional IDs   
            ['ROUTE_AVL'        ,'ROUTE_AVL'     ,'first'   ,'int64'     , 0],          
            ['TRIP_ID'          ,'TRIP_ID'       ,'first'   ,'int64'     , 0],         
            ['STOP_ID'          ,'STOP_ID'       ,'first'   ,'int64'     , 0],           
            ['STOP_AVL'         ,'STOP_AVL'      ,'first'   ,'float64'   , 0],          
            ['BLOCK_ID'         ,'BLOCK_ID'      ,'first'   ,'int64'     , 0],           
            ['SHAPE_ID'         ,'SHAPE_ID'      ,'first'   ,'int64'     , 0],           
            ['SHAPE_DIST'       ,'SHAPE_DIST'    ,'first'   ,'float64'   , 0],           
            ['VEHNO'            ,'VEHNO'         ,'first'   ,'float64'   , 0]     
            ]
                
        # open the data stores
        instore = pd.HDFStore(hdf_infile)
        outstore = pd.HDFStore(hdf_aggfile)
        
        # don't append the data, overwrite
        try: 
            outstore.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = instore.select_column('expanded', 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        daysOfWeek = instore.select_column('expanded', 'DOW').unique()
        print 'For each month, processing %i days of week' % len(daysOfWeek)
        
        timesOfDay = instore.select_column('expanded', 'TOD').unique()
        print 'For each month, processing %i times of day' % len(timesOfDay)
        
        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month
            
            for dow in daysOfWeek: 
                dow = int(dow)
                
                for tod in timesOfDay: 
        
                    # get a months worth of data for this day of week
                    df = instore.select(inkey, where='MONTH==Timestamp(month) and DOW==dow and TOD=tod')
        
                    # count the number of days in this period
                    df['NUMDAYS'] = len(df['DATE'].unique())
                    print '   Processing day of week %i, and time of day %s with with %i unique dates.' % (
                        dow, tod, len(df['DATE'].unique()))
    
                    # nothing is imputed, and weights are all 1                                                
                    df['IMPTRIPS'] = 0
                    df['WEIGHT'] = 1.0
    
                    # normalize times to offset from start of month
                    timeFields = ['ARRIVAL_TIME_S', 
                                'ARRIVAL_TIME', 
                                'DEPARTURE_TIME_S', 
                                'DEPARTURE_TIME'] 
                    for col in timeFields:
                        if col in df: 
                            offset = df[col] - df['DATE']
                            df[col] = month + offset
        
                    # aggregate
                    aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecs)
                    
                    # update speeds
                    aggregated = self.updateSpeeds(aggregated)
        
                    # write
                    outstore.append(outkey, aggregated, data_columns=True, 
                        min_itemsize=stringLengths)
                    
        instore.close()
        outstore.close()
       
    def imputeMissingRecordValues(self, targetDf, sourceDf, columnSpecs, imputedIdentifier):
        """
        Fill missing values in targetDf, with matching values from sourceDf. 
        
        targetDf - dataframe with missing values we want to fill in (will be overwritten)
        sourceDf - dataframe with same specification, but values to draw from
        columnSpecs - where to get each column from.  Can be: 
                      'join' if it is an index field to join between two data sets
                      'keep' if to keep value from targetDf
                      'impute' if to take value from sourceDf
        imputedIdentifier - integer used to fill field 'IMPUTED'
        
        Overwrites same datastore.  
        """
        
        # convert column specs 
        colnames = []   
        stringLengths= {}
        dataTypes = {}
        joinFields = []
        imputedFields = []
        for col in columnSpecs: 
            name = col[0]
            method = col[1]
            dtype = col[2]
            stringLength = col[3]
            
            colnames.append(name)
            dataTypes[name] = dtype
            
            if (stringLength>0): 
                stringLengths[name] = stringLength
            if method=='join': 
                joinFields.append(name)
            if method=='impute':
                imputedFields.append(name)
        
        # if no data in sourceDf, then nothing to do
        if len(sourceDf)==0: 
            print '  Imputing month %i.  No records in source dataframe' % imputedIdentifier
            return targetDf, stringLengths    
            
        # join the data
        joined = pd.merge(targetDf, sourceDf, how='left', on=joinFields, 
                                suffixes=('', '_SOURCE'), sort=True)
        
        # select the records with missing data, and fill in as appropriate
        totalRecords = 0
        missingRecords = 0
        successfulMatches = 0 
        for i, row in joined.iterrows():
            totalRecords += 1
                 
            if (joined['OBSTRIPS'][i]==0 and joined['IMPUTED'][i]==0):
                missingRecords += 1
                
                if (joined['OBSTRIPS_SOURCE'][i]>0):
                    successfulMatches += 1
                    
                    joined['IMPUTED'][i] = imputedIdentifier
                    joined['IMPTRIPS'][i] = joined['OBSTRIPS_SOURCE'][i]
                    for col in imputedFields:
                        if (dataTypes[col]=='datetime64'): 
                            oldTime = pd.Timestamp(joined[col + '_SOURCE'][i])
                            newTime = oldTime + pd.DateOffset(months=-imputedIdentifier)
                            joined[col][i] = newTime
                        else: 
                            joined[col][i] = joined[col + '_SOURCE'][i]
            
        # keep only the relevant columns               
        result = joined[colnames]     

        # report some statistics
        stillMissing = missingRecords - successfulMatches
        print '  Imputing month %i.  For %i total records, started with '\
               '%i missing and finished with %i.' % (
               imputedIdentifier, totalRecords, missingRecords, stillMissing)
        
        return result, stringLengths


    def imputeMissingValuesByMonth(self, hdf_infile, hdf_outfile, inkey, outkey):
        """
        For each month in the monthly average data frame, looks to imput missing
        values based on the same trip occurring the month before or the month after.
        
        Also calculates a WEIGHT, and uses that weight to scale up ridership. 
        The weight accounts for the fact that even after imputating missing values, 
        some trips are still not observed for the month.  Other trips on the same
        route are factored up make up for that.  
        
        hdffile - HDF5 file with data store
        key - name of key with monthly average data
        
        Overwrites same datastore.  
        """
        
        # specify method as 'join' if it is a field to join on with the previous or next month.  
        # specify method as 'keep' to keep value from current month
        # specify as 'impute' to impute value from previous or next month
        #        #   outfield,            method, type, stringLength                
        columnSpecs = [         
            ['MONTH'                 ,'keep'    ,'datetime64', 0],         # monthly aggregations   
            ['DOW'                   ,'keep'    ,'int64'     , 0],
            ['TOD'                   ,'join'    ,'object'    ,10],
            ['NUMDAYS'               ,'keep'    ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'              ,'keep'    ,'int64'     , 0],      
            ['OBSTRIPS'              ,'keep'    ,'float64'   , 0],   
            ['IMPTRIPS'              ,'keep'    ,'float64'   , 0],   
            ['IMPUTED'               ,'keep'    ,'int64'     , 0],  
            ['WEIGHT'                ,'keep'    ,'float64'   , 0], 
            ['AGENCY_ID'             ,'join'    ,'object'    ,10],         # grouping fields        
            ['ROUTE_SHORT_NAME'      ,'join'    ,'object'    ,10],         
            ['ROUTE_LONG_NAME'       ,'join'    ,'object'    ,32],  
            ['DIR'                   ,'join'    ,'int64'     , 0], 
            ['TRIP'                  ,'join'    ,'int64'     , 0], 
            ['SEQ'                   ,'join'    ,'int64'     , 0],       
            ['ROUTE_TYPE'            ,'keep'    ,'int64'     , 0],         # route attributes  
            ['TRIP_HEADSIGN'         ,'keep'    ,'object'    ,64],   
            ['HEADWAY'               ,'keep'    ,'float64'   , 0],   
            ['FARE'                  ,'keep'    ,'float64'   , 0], 
            ['PATTCODE'              ,'impute'  ,'object'    ,10],  
            ['SCHOOL'                ,'impute'  ,'int64'     , 0],    
            ['STOPNAME'              ,'keep'    ,'object'    ,32],         # stop attributes
            ['STOPNAME_AVL'          ,'impute'  ,'object'    ,32],  
            ['STOP_LAT'              ,'keep'    ,'float64'   , 0],   
            ['STOP_LON'              ,'keep'    ,'float64'   , 0],   
            ['EOL'                   ,'keep'    ,'int64'     , 0],   
            ['SOL'                   ,'keep'    ,'int64'     , 0],   
            ['TIMEPOINT'             ,'impute'  ,'int64'     , 0],  
            ['ARRIVAL_TIME_S'        ,'impute'  ,'datetime64',0],         # times   
            ['ARRIVAL_TIME'          ,'impute'  ,'datetime64',0],      
            ['ARRIVAL_TIME_DEV'      ,'impute'  ,'float64'  , 0],   
            ['DEPARTURE_TIME_S'      ,'impute'  ,'datetime64', 0],  
            ['DEPARTURE_TIME'        ,'impute'  ,'datetime64', 0],  
            ['DEPARTURE_TIME_DEV'    ,'impute'  ,'float64'   , 0],   
            ['DWELL_S'               ,'keep'    ,'float64'   , 0],
            ['DWELL'                 ,'impute'  ,'float64'   , 0],    
            ['RUNTIME_S'             ,'keep'    ,'float64'   , 0],
            ['RUNTIME'               ,'impute'  ,'float64'   , 0],    
            ['SERVMILES'             ,'keep'    ,'float64'   , 0],
            ['SERVMILES_AVL'         ,'impute'  ,'float64'   , 0],    
            ['RUNSPEED_S'            ,'keep'    ,'float64'   , 0],
            ['RUNSPEED'              ,'impute'  ,'float64'   , 0],                 
            ['ONTIME4'               ,'impute'  ,'float64'   , 0],   
            ['ONTIME10'              ,'impute'  ,'float64'   , 0],       
            ['ON'                    ,'impute'  ,'float64'   , 0],          # ridership 
            ['OFF'                   ,'impute'  ,'float64'   , 0],   
            ['LOAD_ARR'              ,'impute'  ,'float64'   , 0],   
            ['LOAD_DEP'              ,'impute'  ,'float64'   , 0],          
            ['PASSMILES'             ,'impute'  ,'float64'   , 0],   
            ['PASSHOURS'             ,'impute'  ,'float64'   , 0],   
            ['WAITHOURS'             ,'impute'  ,'float64'   , 0],   
            ['PASSDELAY_DEP'         ,'impute'  ,'float64'   , 0],   
            ['PASSDELAY_ARR'         ,'impute'  ,'float64'   , 0],    
            ['RDBRDNGS'              ,'impute'  ,'float64'   , 0],   
            ['CAPACITY'              ,'impute'  ,'float64'   , 0],   
            ['DOORCYCLES'            ,'impute'  ,'float64'   , 0],   
            ['WHEELCHAIR'            ,'impute'  ,'float64'   , 0],   
            ['BIKERACK'              ,'impute'  ,'float64'   , 0],   
            ['VC'                    ,'impute'  ,'float64'   , 0],        # crowding
            ['CROWDED'               ,'impute'  ,'float64'   , 0],   
            ['CROWDHOURS'            ,'impute'  ,'float64'   , 0], 
            #['ARRIVAL_TIME_DEV_STD'  ,'impute'  ,'float64'   , 0],         # standard deviations      
            #['DEPARTURE_TIME_DEV_STD','impute'  ,'float64'   , 0],       
            #['DWELL_STD'             ,'impute'  ,'float64'   , 0],     
            #['RUNTIME_STD'           ,'impute'  ,'float64'   , 0],    
            #['RUNSPEED_STD'          ,'impute'  ,'float64'   , 0],                
            #['ON_STD'                ,'impute'  ,'float64'   , 0],  
            #['OFF_STD'               ,'impute'  ,'float64'   , 0],  
            #['LOAD_ARR_STD'          ,'impute'  ,'float64'   , 0],  
            #['LOAD_DEP_STD'          ,'impute'  ,'float64'   , 0],   
            #['PASSMILES_STD'         ,'impute'  ,'float64'   , 0],  
            #['PASSHOURS_STD'         ,'impute'  ,'float64'   , 0],   
            #['WAITHOURS_STD'         ,'impute'  ,'float64'   , 0],  
            #['PASSDELAY_DEP_STD'     ,'impute'  ,'float64'   , 0],  
            #['PASSDELAY_ARR_STD'     ,'impute'  ,'float64'   , 0],     
            #['RDBRDNGS_STD'          ,'impute'  ,'float64'   , 0],  
            #['DOORCYCLES_STD'        ,'impute'  ,'float64'   , 0],  
            #['WHEELCHAIR_STD'        ,'impute'  ,'float64'   , 0],  
            #['BIKERACK_STD'          ,'impute'  ,'float64'   , 0],  
            ['ROUTE_ID'              ,'keep'    ,'int64'     , 0],         # additional IDs   
            ['ROUTE_AVL'             ,'impute'  ,'int64'     , 0],          
            ['TRIP_ID'               ,'keep'    ,'int64'     , 0],         
            ['STOP_ID'               ,'keep'    ,'int64'     , 0],           
            ['STOP_AVL'              ,'impute'  ,'float64'   , 0],          
            ['BLOCK_ID'              ,'keep'    ,'int64'     , 0],           
            ['SHAPE_ID'              ,'keep'    ,'int64'     , 0],           
            ['SHAPE_DIST'            ,'keep'    ,'float64'   , 0],           
            ['VEHNO'                 ,'impute'  ,'float64'   , 0]     
            ]
              
        # open the data stores
        instore = pd.HDFStore(hdf_infile)        
        outstore = pd.HDFStore(hdf_outfile)       

        # don't append the data, overwrite
        try: 
            outstore.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = instore.select_column(inkey, 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        daysOfWeek = instore.select_column(inkey, 'DOW').unique()
        print 'For each month, processing %i days of week' % len(daysOfWeek)

        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month            
            
            for dow in daysOfWeek: 
                print ' Processing day of week ', dow
                dow = int(dow)
    
                # get a months worth of data for this day of week
                df = instore.select(inkey, where='MONTH==Timestamp(month) and DOW==dow')
                
                # skip if there are no records
                if (len(df)==0):
                    print '0 records, so skipping to next day of week/month'
                else: 
                    
                    df['IMPUTED'] = 0
                    
                    # go up to two months in either direction out looking for a match
                    for i in range(1, 3): 
    
                        lastMonth = pd.Timestamp(month) + pd.DateOffset(months=-i)
                        nextMonth = pd.Timestamp(month) + pd.DateOffset(months=i)
                                    
                        # try filling in missing values with data from previous month
                        # set value for IMPUTED field to -i
                        sourceDf = instore.select(inkey, where='MONTH==Timestamp(lastMonth) and DOW==dow and OBSTRIPS>=1')
                        df, stringLengths = self.imputeMissingRecordValues(df, sourceDf, columnSpecs, -i) 
                    
                        # try filling in missing values with data from next month
                        # set value for IMPUTED field to i
                        sourceDf = instore.select(inkey, where='MONTH==Timestamp(nextMonth) and DOW==dow and OBSTRIPS>=1')
                        df, stringLegnths = self.imputeMissingRecordValues(df, sourceDf, columnSpecs, i) 
    
                    # calculate the WEIGHT
                    print '  calculating weights.'
                    groupby = ['AGENCY_ID','ROUTE_SHORT_NAME','ROUTE_LONG_NAME','DIR','SEQ']
                    df = df.groupby(groupby).apply(calculateWeight)
    
                    # scale up the ridership based on weight   
                    # TODO - scale standard deviations too?             
                    df['ON']            = df['WEIGHT'] * df['ON']          
                    df['OFF']           = df['WEIGHT'] * df['OFF']        
                    df['LOAD_ARR']      = df['WEIGHT'] * df['LOAD_ARR']   
                    df['LOAD_DEP']      = df['WEIGHT'] * df['LOAD_DEP']   
                    df['PASSMILES']     = df['WEIGHT'] * df['PASSMILES']  
                    df['PASSHOURS']     = df['WEIGHT'] * df['PASSHOURS']  
                    df['WAITHOURS']     = df['WEIGHT'] * df['WAITHOURS']  
                    df['PASSDELAY_DEP'] = df['WEIGHT'] * df['PASSDELAY_DEP']  
                    df['PASSDELAY_ARR'] = df['WEIGHT'] * df['PASSDELAY_ARR']  
                    df['RDBRDNGS']      = df['WEIGHT'] * df['RDBRDNGS']   
                    df['CAPACITY']      = df['WEIGHT'] * df['CAPACITY']   
                    df['DOORCYCLES']    = df['WEIGHT'] * df['DOORCYCLES'] 
                    df['WHEELCHAIR']    = df['WEIGHT'] * df['WHEELCHAIR'] 
                    df['BIKERACK']      = df['WEIGHT'] * df['BIKERACK']    
                    #df['VC'] is a ratio, so no need to scale
                    #df['CROWDED'] is based on a ratio, so no need to scale
                    df['CROWDHOURS']    = df['WEIGHT'] * df['CROWDHOURS']    
        
                    # write
                    print '  writing data.'
                    outstore.append(outkey, df, data_columns=True, 
                        min_itemsize=stringLengths)
                    
        instore.close()
                    

    def calculateRouteStopTotals(self, hdffile, inkey, outkey):
        """
        Read disaggregate transit records, and aggregates across trips to a
        daily total. 
        
        hdffile - HDF5 file to aggregate
        inkey   - string - key for reading detailed data from
        outkey  - string - key for writing the aggregated dataframe to the store
                                                              
        """

        # specify 'groupby' as aggregation method as appropriate
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #   outfield,            infield,  aggregationMethod, type, stringLength                
        columnSpecs = [              
            ['MONTH'            ,'MONTH'         ,'first'   ,'datetime64', 0],         # monthly aggregations   
            ['DOW'              ,'DOW'           ,'groupby' ,'int64'     , 0],
            ['TOD'              ,'TOD'           ,'groupby' ,'object'    ,10],
            ['NUMDAYS'          ,'NUMDAYS'       ,'first'   ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'         ,'TOTTRIPS'      ,'sum'     ,'int64'     , 0],      
            ['OBSTRIPS'         ,'OBSTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['IMPTRIPS'         ,'IMPTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['WEIGHT'           ,'WEIGHT'        ,'mean'    ,'float64'   , 0], 
            ['NUMSTOPS'         ,'none'          ,'count'   ,'int64'     , 0],      
   	    ['AGENCY_ID'        ,'AGENCY_ID'     ,'groupby'  ,'object'   ,10],         # grouping fields        
   	    ['ROUTE_SHORT_NAME' ,'ROUTE_SHORT_NAME','groupby','object'   ,10],         
   	    ['ROUTE_LONG_NAME'  ,'ROUTE_LONG_NAME','groupby' ,'object'   ,32],  
            ['DIR'              ,'DIR'           ,'groupby' ,'int64'     , 0], 
            ['SEQ'              ,'SEQ'           ,'groupby' ,'int64'     , 0],       
            ['ROUTE_TYPE'       ,'ROUTE_TYPE'    ,'first'   ,'int64'     , 0],         # route attributes  
            ['TRIP_HEADSIGN'    ,'TRIP_HEADSIGN' ,'first'   ,'object'    ,64],   
            ['HEADWAY'          ,'HEADWAY'       ,'mean'    ,'float64'   , 0],   
            ['FARE'             ,'FARE'          ,'mean'    ,'float64'   , 0], 
	    ['SCHOOL'           ,'SCHOOL'        ,'first'   ,'int64'     , 0],    
            ['STOPNAME'         ,'STOPNAME'      ,'first'   ,'object'    ,32],         # stop attributes
            ['STOPNAME_AVL'     ,'STOPNAME_AVL'  ,'first'   ,'object'    ,32],  
            ['STOP_LAT'         ,'STOP_LAT'      ,'first'   ,'float64'   , 0],   
            ['STOP_LON'         ,'STOP_LON'      ,'first'   ,'float64'   , 0],   
            ['EOL'              ,'EOL'           ,'first'   ,'int64'     , 0],   
            ['SOL'              ,'SOL'           ,'first'   ,'int64'     , 0],   
            ['TIMEPOINT'        ,'TIMEPOINT'     ,'first'   ,'int64'     , 0],     
            ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean','float64'  , 0],        # times 
            ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean','float64'  , 0],   
            ['DWELL_S'          ,'DWELL_S'       ,'sum'     ,'float64'   , 0],
            ['DWELL'            ,'DWELL'         ,'sum'     ,'float64'   , 0],    
            ['RUNTIME_S'        ,'RUNTIME_S'     ,'sum'     ,'float64'   , 0],
            ['RUNTIME'          ,'RUNTIME'       ,'sum'     ,'float64'   , 0],   
            ['SERVMILES_S'      ,'SERVMILES_S'   ,'sum'     ,'float64'   , 0],
            ['SERVMILES'        ,'SERVMILES'     ,'sum'     ,'float64'   , 0],
            ['RUNSPEED_S'       ,'RUNSPEED_S'    ,'mean'    ,'float64'   , 0],
            ['RUNSPEED'         ,'RUNSPEED'      ,'mean'    ,'float64'   , 0],                 
            ['ONTIME4'          ,'ONTIME4'       ,'mean'    ,'float64'   , 0],   
            ['ONTIME10'         ,'ONTIME10'      ,'mean'    ,'float64'   , 0],              
            ['ON'               ,'ON'            ,'sum'     ,'float64'   , 0],          # ridership   
            ['OFF'              ,'OFF'           ,'sum'     ,'float64'   , 0],   
            ['LOAD_ARR'         ,'LOAD_ARR'      ,'sum'     ,'float64'   , 0],   
            ['LOAD_DEP'         ,'LOAD_DEP'      ,'sum'     ,'float64'   , 0],            
            ['PASSMILES'        ,'PASSMILES'     ,'sum'     ,'float64'   , 0],   
            ['PASSHOURS'        ,'PASSHOURS'     ,'sum'     ,'float64'   , 0],  
            ['WAITHOURS'        ,'WAITHOURS'     ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_DEP'    ,'PASSDELAY_DEP' ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_ARR'    ,'PASSDELAY_ARR' ,'sum'     ,'float64'   , 0],   
            ['RDBRDNGS'         ,'RDBRDNGS'      ,'sum'     ,'float64'   , 0],   
            ['CAPACITY'         ,'CAPACITY'      ,'sum'     ,'float64'   , 0],   
            ['DOORCYCLES'       ,'DOORCYCLES'    ,'sum'     ,'float64'   , 0],   
            ['WHEELCHAIR'       ,'WHEELCHAIR'    ,'sum'     ,'float64'   , 0],  
            ['BIKERACK'         ,'BIKERACK'      ,'sum'     ,'float64'   , 0],  
            ['VC'               ,'VC'            ,'mean'    ,'float64'   , 0],        # crowding
            ['CROWDED'          ,'CROWDED'       ,'mean'    ,'float64'   , 0],   
            ['CROWDHOURS'       ,'CROWDHOURS'    ,'sum'     ,'float64'   , 0],  
            ['ROUTE_ID'         ,'ROUTE_ID'      ,'first'   ,'int64'     , 0],         # additional IDs   
            ['ROUTE_AVL'        ,'ROUTE_AVL'     ,'first'   ,'int64'     , 0],        
            ['STOP_ID'          ,'STOP_ID'       ,'first'   ,'int64'     , 0],           
            ['STOP_AVL'         ,'STOP_AVL'      ,'first'   ,'float64'   , 0],          
            ['BLOCK_ID'         ,'BLOCK_ID'      ,'first'   ,'int64'     , 0]   
            ]
        
        # for calculating daily totals
        columnSpecsDaily = columnSpecs
        for i in range(0, len(columnSpecsDaily)):
            if columnSpecsDaily[i][0] == 'TOD':
                columnSpecsDaily[i][2] = 'first'

        # open the data stores
        store = pd.HDFStore(hdffile)
        
        # only append if we're calculating the daily totals
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = store.select_column(inkey, 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month
                
            # get a months worth of data for this day of week
            df = store.select(inkey, where='MONTH==Timestamp(month)')
            
            # apply weights to SERVMILES_S, DWELL, RUNTIME,
            # because they were not weighted previously. 
            df['SERVMILES_S'] = df['SERVMILES']  # unweighted, but for everything
            df['SERVMILES']     = df['WEIGHT'] * df['SERVMILES']
            df['DWELL']         = df['WEIGHT'] * df['DWELL']
            df['RUNTIME']       = df['WEIGHT'] * df['RUNTIME']
                                                            
            # aggregate
            aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecs)
                    
            # update speeds
            aggregated = self.updateSpeeds(aggregated)
                
            # write
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=stringLengths)
                
            # now calculate a daily total, and append that
            aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecsDaily)
            aggregated = self.updateSpeeds(aggregated)
            aggregated['TOD'] = 'Daily'
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=stringLengths)
                                
        store.close()


    def calculateRouteTotals(self, hdffile, inkey, outkey):
        """
        Sum across stops to get route totals
        
        hdffile - HDF5 file to aggregate
        inkey   - string - key for reading detailed data from
        outkey  - string - key for writing the aggregated dataframe to the store
                                   
        """


        # specify 'groupby' as aggregation method as appropriate
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #   outfield,            infield,  aggregationMethod, type, stringLength                
        columnSpecs = [              
            ['MONTH'            ,'MONTH'         ,'first'   ,'datetime64', 0],         # monthly aggregations   
            ['DOW'              ,'DOW'           ,'groupby' ,'int64'     , 0],
            ['TOD'              ,'TOD'           ,'groupby' ,'object'    ,10],
            ['NUMDAYS'          ,'NUMDAYS'       ,'first'   ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'         ,'TOTTRIPS'      ,'max'     ,'int64'     , 0],      
            ['OBSTRIPS'         ,'OBSTRIPS'      ,'max'     ,'float64'   , 0], 
            ['IMPTRIPS'         ,'IMPTRIPS'      ,'max'     ,'float64'   , 0], 
            ['WEIGHT'           ,'WEIGHT'        ,'mean'    ,'float64'   , 0], 
            ['NUMSTOPS'         ,'NUMSTOPS'      ,'sum'     ,'int64'     , 0],   
   	    ['AGENCY_ID'        ,'AGENCY_ID'     ,'groupby'  ,'object'   ,10],         # grouping fields        
   	    ['ROUTE_SHORT_NAME' ,'ROUTE_SHORT_NAME','groupby','object'   ,10],         
   	    ['ROUTE_LONG_NAME'  ,'ROUTE_LONG_NAME','groupby' ,'object'   ,32],  
            ['DIR'              ,'DIR'           ,'groupby' ,'int64'     , 0],       
            ['ROUTE_TYPE'       ,'ROUTE_TYPE'    ,'first'   ,'int64'     , 0],         # route attributes  
            ['TRIP_HEADSIGN'    ,'TRIP_HEADSIGN' ,'first'   ,'object'    ,64],   
            ['HEADWAY'          ,'HEADWAY'       ,'mean'    ,'float64'   , 0],   
            ['FARE'             ,'FARE'          ,'mean'    ,'float64'   , 0], 
	    ['SCHOOL'           ,'SCHOOL'        ,'first'   ,'int64'     , 0],    
            ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean','float64'  , 0],        # times 
            ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean','float64'  , 0],   
            ['DWELL_S'          ,'DWELL_S'       ,'sum'     ,'float64'   , 0],
            ['DWELL'            ,'DWELL'         ,'sum'     ,'float64'   , 0],    
            ['RUNTIME_S'        ,'RUNTIME_S'     ,'sum'     ,'float64'   , 0],
            ['RUNTIME'          ,'RUNTIME'       ,'sum'     ,'float64'   , 0],  
            ['SERVMILES_S'      ,'SERVMILES_S'   ,'sum'     ,'float64'   , 0], 
            ['SERVMILES'        ,'SERVMILES'     ,'sum'     ,'float64'   , 0],
            ['RUNSPEED_S'       ,'RUNSPEED_S'    ,'mean'    ,'float64'   , 0],
            ['RUNSPEED'         ,'RUNSPEED'      ,'mean'    ,'float64'   , 0],                 
            ['ONTIME4'          ,'ONTIME4'       ,'mean'    ,'float64'   , 0],   
            ['ONTIME10'         ,'ONTIME10'      ,'mean'    ,'float64'   , 0],              
            ['ON'               ,'ON'            ,'sum'     ,'float64'   , 0],          # ridership   
            ['OFF'              ,'OFF'           ,'sum'     ,'float64'   , 0],   
            ['LOAD_ARR'         ,'LOAD_ARR'      ,'sum'     ,'float64'   , 0],   
            ['LOAD_DEP'         ,'LOAD_DEP'      ,'sum'     ,'float64'   , 0],            
            ['PASSMILES'        ,'PASSMILES'     ,'sum'     ,'float64'   , 0],   
            ['PASSHOURS'        ,'PASSHOURS'     ,'sum'     ,'float64'   , 0], 
            ['WAITHOURS'        ,'WAITHOURS'     ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_DEP'    ,'PASSDELAY_DEP' ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_ARR'    ,'PASSDELAY_ARR' ,'sum'     ,'float64'   , 0],     
            ['RDBRDNGS'         ,'RDBRDNGS'      ,'sum'     ,'float64'   , 0],   
            ['CAPACITY'         ,'CAPACITY'      ,'sum'     ,'float64'   , 0],   
            ['DOORCYCLES'       ,'DOORCYCLES'    ,'sum'     ,'float64'   , 0],   
            ['WHEELCHAIR'       ,'WHEELCHAIR'    ,'sum'     ,'float64'   , 0],  
            ['BIKERACK'         ,'BIKERACK'      ,'sum'     ,'float64'   , 0],   
            ['VC'               ,'VC'            ,'max'     ,'float64'   , 0],         # crowding
            ['CROWDED'          ,'CROWDED'       ,'max'     ,'float64'   , 0],   
            ['CROWDHOURS'       ,'CROWDHOURS'    ,'sum'     ,'float64'   , 0],  
            ['ROUTE_ID'         ,'ROUTE_ID'      ,'first'   ,'int64'     , 0],         # additional IDs   
            ['ROUTE_AVL'        ,'ROUTE_AVL'     ,'first'   ,'int64'     , 0]  
            ]

        # open the data stores
        store = pd.HDFStore(hdffile)
        
        # only append if we're calculating the daily totals
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = store.select_column(inkey, 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month
                
            # get a months worth of data for this day of week
            df = store.select(inkey, where='MONTH==Timestamp(month)')
                                                
            # aggregate
            aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecs)
    
            # update speeds
            aggregated = self.updateSpeeds(aggregated)
                
            # write
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=stringLengths)
                    
        store.close()


    def calculateStopTotals(self, hdffile, inkey, outkey):
        """
        Aggregates across routes to get totals at each stop. 
        
        hdffile - HDF5 file to aggregate
        inkey   - string - key for reading detailed data from
        outkey  - string - key for writing the aggregated dataframe to the store
                                   
        """


        # specify 'groupby' as aggregation method as appropriate
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #   outfield,            infield,  aggregationMethod, type, stringLength                
        columnSpecs = [              
            ['MONTH'            ,'MONTH'         ,'first'   ,'datetime64', 0],         # monthly aggregations   
            ['DOW'              ,'DOW'           ,'groupby' ,'int64'     , 0],
            ['TOD'              ,'TOD'           ,'groupby' ,'object'    ,10],
            ['NUMDAYS'          ,'NUMDAYS'       ,'first'   ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'         ,'TOTTRIPS'      ,'sum'     ,'int64'     , 0],      
            ['OBSTRIPS'         ,'OBSTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['IMPTRIPS'         ,'IMPTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['WEIGHT'           ,'WEIGHT'        ,'mean'    ,'float64'   , 0],       
            ['NUMSTOPS'         ,'NUMSTOPS'      ,'sum'     ,'int64'     , 0],   
            ['STOP_ID'          ,'STOP_ID'       ,'groupby' ,'int64'     , 0],         # group by stop ID        
            ['STOP_AVL'         ,'STOP_AVL'      ,'first'   ,'float64'   , 0], 
   	    ['HEADWAY'          ,'HEADWAY'       ,'mean'    ,'float64'   , 0],   
            ['FARE'             ,'FARE'          ,'mean'    ,'float64'   , 0], 
	    ['SCHOOL'           ,'SCHOOL'        ,'first'   ,'int64'     , 0],    
            ['STOPNAME'         ,'STOPNAME'      ,'first'   ,'object'    ,32],         # stop attributes
            ['STOPNAME_AVL'     ,'STOPNAME_AVL'  ,'first'   ,'object'    ,32],  
            ['STOP_LAT'         ,'STOP_LAT'      ,'first'   ,'float64'   , 0],   
            ['STOP_LON'         ,'STOP_LON'      ,'first'   ,'float64'   , 0],   
            ['EOL'              ,'EOL'           ,'mean'    ,'int64'     , 0],   
            ['SOL'              ,'SOL'           ,'mean'    ,'int64'     , 0],   
            ['TIMEPOINT'        ,'TIMEPOINT'     ,'mean'    ,'int64'     , 0],     
            ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean','float64'  , 0],        # times 
            ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean','float64'  , 0],   
            ['DWELL_S'          ,'DWELL_S'       ,'sum'     ,'float64'   , 0],
            ['DWELL'            ,'DWELL'         ,'sum'     ,'float64'   , 0],                 
            ['ONTIME4'          ,'ONTIME4'       ,'mean'    ,'float64'   , 0],   
            ['ONTIME10'         ,'ONTIME10'      ,'mean'    ,'float64'   , 0],              
            ['ON'               ,'ON'            ,'sum'     ,'float64'   , 0],        # ridership   
            ['OFF'              ,'OFF'           ,'sum'     ,'float64'   , 0], 
            ['WAITHOURS'        ,'WAITHOURS'     ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_DEP'    ,'PASSDELAY_DEP' ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_ARR'    ,'PASSDELAY_ARR' ,'sum'     ,'float64'   , 0],     
            ['RDBRDNGS'         ,'RDBRDNGS'      ,'sum'     ,'float64'   , 0],   
            ['DOORCYCLES'       ,'DOORCYCLES'    ,'sum'     ,'float64'   , 0],   
            ['WHEELCHAIR'       ,'WHEELCHAIR'    ,'sum'     ,'float64'   , 0],  
            ['BIKERACK'         ,'BIKERACK'      ,'sum'     ,'float64'   , 0]
            ]

        # open the data stores
        store = pd.HDFStore(hdffile)
        
        # only append if we're calculating the daily totals
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = store.select_column(inkey, 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month
                
            # get a months worth of data for this day of week
            df = store.select(inkey, where='MONTH==Timestamp(month)')
                                                
            # aggregate
            aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecs)
    
            # update speeds -- no speeds in stop file
            #aggregated = self.updateSpeeds(aggregated)
                
            # write
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=stringLengths)
                    
        store.close()



    def calculateSystemTotals(self, hdffile, inkey, outkey):
        """
        Sum across stops to get system totals
        
        hdffile - HDF5 file to aggregate
        inkey   - string - key for reading detailed data from
        outkey  - string - key for writing the aggregated dataframe to the store
        """                           

        # specify 'groupby' as aggregation method as appropriate
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #   outfield,            infield,  aggregationMethod, type, stringLength                
        columnSpecs = [              
            ['MONTH'            ,'MONTH'         ,'first'   ,'datetime64', 0],         # monthly aggregations   
            ['DOW'              ,'DOW'           ,'groupby' ,'int64'     , 0],
            ['TOD'              ,'TOD'           ,'groupby' ,'object'    ,10],
            ['NUMDAYS'          ,'NUMDAYS'       ,'first'   ,'int64'     , 0],         # stats for observations
            ['TOTTRIPS'         ,'TOTTRIPS'      ,'sum'     ,'int64'     , 0],      
            ['OBSTRIPS'         ,'OBSTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['IMPTRIPS'         ,'IMPTRIPS'      ,'sum'     ,'float64'   , 0], 
            ['WEIGHT'           ,'WEIGHT'        ,'mean'    ,'float64'   , 0], 
            ['NUMSTOPS'         ,'NUMSTOPS'      ,'sum'     ,'int64'     , 0],  
   	    ['AGENCY_ID'        ,'AGENCY_ID'     ,'groupby'  ,'object'   ,10],         # grouping fields 
            ['HEADWAY'          ,'HEADWAY'       ,'mean'    ,'float64'   , 0],   
            ['FARE'             ,'FARE'          ,'mean'    ,'float64'   , 0], 
            ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'mean','float64'  , 0],         # times 
            ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','mean','float64'  , 0],   
            ['DWELL_S'          ,'DWELL_S'       ,'sum'     ,'float64'   , 0],
            ['DWELL'            ,'DWELL'         ,'sum'     ,'float64'   , 0],    
            ['RUNTIME_S'        ,'RUNTIME_S'     ,'sum'     ,'float64'   , 0],
            ['RUNTIME'          ,'RUNTIME'       ,'sum'     ,'float64'   , 0],    
            ['SERVMILES_S'      ,'SERVMILES_S'   ,'sum'     ,'float64'   , 0], 
            ['SERVMILES'        ,'SERVMILES'     ,'sum'     ,'float64'   , 0], 
            ['RUNSPEED_S'       ,'RUNSPEED_S'    ,'mean'    ,'float64'   , 0],
            ['RUNSPEED'         ,'RUNSPEED'      ,'mean'    ,'float64'   , 0],                 
            ['ONTIME4'          ,'ONTIME4'       ,'mean'    ,'float64'   , 0],   
            ['ONTIME10'         ,'ONTIME10'      ,'mean'    ,'float64'   , 0],              
            ['ON'               ,'ON'            ,'sum'     ,'float64'   , 0],          # ridership   
            ['OFF'              ,'OFF'           ,'sum'     ,'float64'   , 0],           
            ['PASSMILES'        ,'PASSMILES'     ,'sum'     ,'float64'   , 0],   
            ['PASSHOURS'        ,'PASSHOURS'     ,'sum'     ,'float64'   , 0], 
            ['WAITHOURS'        ,'WAITHOURS'     ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_DEP'    ,'PASSDELAY_DEP' ,'sum'     ,'float64'   , 0],   
            ['PASSDELAY_ARR'    ,'PASSDELAY_ARR' ,'sum'     ,'float64'   , 0],     
            ['RDBRDNGS'         ,'RDBRDNGS'      ,'sum'     ,'float64'   , 0],   
            ['CAPACITY'         ,'CAPACITY'      ,'sum'     ,'float64'   , 0],   
            ['DOORCYCLES'       ,'DOORCYCLES'    ,'sum'     ,'float64'   , 0],   
            ['WHEELCHAIR'       ,'WHEELCHAIR'    ,'sum'     ,'float64'   , 0],  
            ['BIKERACK'         ,'BIKERACK'      ,'sum'     ,'float64'   , 0],
            ['VC'               ,'VC'            ,'mean'    ,'float64'   , 0],         # crowding
            ['CROWDED'          ,'CROWDED'       ,'mean'    ,'float64'   , 0],   
            ['CROWDHOURS'       ,'CROWDHOURS'    ,'sum'     ,'float64'   , 0]  
            ]

        # open the data stores
        store = pd.HDFStore(hdffile)
        
        # only append if we're calculating the daily totals
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey

        # get the list of months and days of week to loop through
        months = store.select_column(inkey, 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        # loop through the months, and days of week
        for month in months: 
            print 'Processing ', month
                
            # get a months worth of data for this day of week
            df = store.select(inkey, where='MONTH==Timestamp(month)')
                                                
            # aggregate
            aggregated, stringLengths = self.aggregateTransitRecords(df, columnSpecs)
    
            # update speeds
            aggregated = self.updateSpeeds(aggregated)
                
            # write
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=stringLengths)
                    
        store.close()


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
        

    def updateSpeeds(self, df):
        """
        Calculates the speed based on the runtime and distance
        
        dataframe - a series of Datetime objects, must have fields:   
            RUNTIME_S
            RUNTIME        
            SERVMILES_S   
            SERVMILES   
            RUNSPEED_S
            RUNSPEED
        
        returns the same dataframe, overwritten
                                   
        """

        for i, row in df.iterrows():    
            # FOR DEBUGGING ONLY
            if (df['RUNTIME'][i]>(10 * df['RUNTIME_S'][i])): 
                df['RUNTIME'][i] = df['RUNTIME_S'][i]
            
            # REGULAR CODE
            if (df['RUNTIME_S'][i]>0): 
                if 'SERVMILES_S' in df: 
                    df['RUNSPEED_S'][i] = round(df['SERVMILES_S'][i] / (df['RUNTIME_S'][i]/60.0), 2)
                else: 
                    df['RUNSPEED_S'][i] = round(df['SERVMILES'][i] / (df['RUNTIME_S'][i]/60.0), 2)
            else: 
                df['RUNSPEED_S'][i] = 0

            if (df['RUNTIME'][i]>0): 
                df['RUNSPEED'][i] = round(df['SERVMILES'][i] / (df['RUNTIME'][i]/60.0), 2)
            else: 
                df['RUNSPEED'][i] = 0

        return df
        