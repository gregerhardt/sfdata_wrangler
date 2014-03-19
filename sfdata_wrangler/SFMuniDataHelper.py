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
    CHUNKSIZE = 100000

    # by default, read the first 75 columns, through NEXTTRIP
    COLUMNS_TO_READ = [i for i in range(75)]

    # specifies columns for fixed-format text input
    COLSPECS=[  (  0,   5), # SEQ     - stop sequence
		(  6,  10), # 'V2'       - not used
		( 10,  14), # 'QSTOP'    - unique stop no	
		( 15,  47), # 'STOPNAME' - stop name
		( 48,  54), # 'TIMESTOP_INT' - arrival time
		( 55,  58), # 'ON'       - on 
		( 59,  62), # 'OFF'      - off
		( 63,  66), # 'LOAD_DEP' - departing load
		( 67,  67), # 'LOADCODE' - ADJ=*, BAL=B
		( 68,  74), # 'DATE_INT' - date
		( 75,  79), # 'ROUTE'    
		( 80,  86), # 'PATTERN'  - schedule pattern
		( 87,  93), # 'BLOCK'  
		( 94, 102), # 'LAT'      - latitude
		(103, 112), # 'LON'      - longitude 
		(113, 118), # 'MILES'    - odometer reading (miles)
		(119, 123), # 'TRIP'     - trip
		(124, 125), # 'DOORCYCLES'- door cycles
		(126, 130), # 'DELTA'    - delta
		(131, 132), # 'DOW'      - day of week -- input data is not reliable
		(133, 134), # 'DIR'      
		(135, 140), # 'VEHMILES' - delta vehicle miles  - miles bus travels from last stop
		(141, 145), # 'DLPMIN'   - delta minutes
		(146, 153), # 'PASSMILES'- delta passenger miles
		(154, 160), # 'PASSHOURS'- delta passenger minutes
		(161, 165), # 'VEHNO'    - bus number
		(166, 170), # 'LINE'     - route (APC numeric code)
		(171, 175), # 'DBNN'     - data batch
		(176, 180), # 'TIMESTOP_S_INT' - schedule time
		(181, 186), # 'RUNTIME_S'- schedule run time, in decimal minutes
		(187, 192), # 'RUNTIME'  - runtime from the last schedule point--TIMESTOP - DOORCLOSE of previous time point. (Excludes DWELL at the time points.), in decimal minutes
		(193, 198), # 'ODOM'     - not used
		(199, 204), # 'GODOM'    - distance (GPS)
		(205, 211), # 'TIMESTOP_DEV'- schedule deviation
		(212, 217), # 'DWELL'    - dwell time interval (decimal minutes) -- (DOORCLOSE - TIMESTOP)
		(218, 226), # 'MSFILE'   - sign up YYMM
		(227, 230), # 'QC101'    - not used
		(231, 234), # 'QC104'    - GPS QC
		(235, 238), # 'QC201'    - count QC
		(239, 242), # 'AQC'      - assignment QC
		(243, 244), # 'RECORD'   - record type
		(245, 246), # 'WHEELCHAIR'- wheelchair
		(247, 248), # 'BIKERACK' - bike rack
		(249, 250), # 'SP2'      - not used
		(251, 257), # 'V51'      - not used
		(258, 263), # 'VERSN'    - import version
		(264, 270), # 'DOORCLOSE_INT'   - departure time
		(271, 274), # 'UON'      - unadjusted on
		(275, 278), # 'UOFF'     - unadjusted off
		(279, 283), # 'CAPACITY' - capacity
		(284, 288), # 'OVER'     - 5 over cap
		(289, 290), # 'NS'       - north/south
		(291, 292), # 'EW'       - east/west
		(293, 296), # 'MAXVEL'   - max velocity on previous link
		(297, 301), # 'RDBRDNGS' - rear door boardings (if 4 digits, the remaining columns get mis-aligned)
		(302, 304), # 'DV'       - division
		(305, 315), # 'PATTCODE' - pattern code
		(316, 320), # 'DWDI'     - distance traveled durign dwell
		(321, 328), # 'RUN'      - run
		(329, 335), # 'SCHOOL'   - school trip
		(336, 344), # 'TRIPID_2' - long trip ID
		(345, 351), # 'PULLOUT_INT'  - movement time
		(352, 356), # 'DOORCLOSE_S_INT'- scheduled departure time
		(357, 363), # 'DOORCLOSE_DEV' - schedule deviation
		(364, 368), # 'DWELL_S'  - scheduled dwell time
		(369, 374), # 'RECOVERY_S'- scheduled EOL recovery
		(375, 380), # 'RECOVERY'     
		(381, 390), # 'POLITICAL'- not used
		(391, 397), # 'DELTAA'   - distance from stop at arrival
		(398, 404), # 'DELTAD'   - distance from stop at departure
		(405, 409), # 'ECNT'     - error count
		(410, 412), # 'MC'       - municipal code
		(413, 416), # 'DIV'      - division
		(417, 421), # 'LASTTRIP' - previous trip
		(422, 426), # 'NEXTTRIP' - next trip
		(427, 430), # 'V86'      - not used
		(431, 441), # 'TRIPID_3' 
		(442, 445), # 'WCC'      
		(446, 449), # 'BRC'      
		(450, 455), # 'DWELLI'   
		(456, 459), # 'QC202'    
		(460, 463), # 'QC302'    
		(464, 467), # 'QC303'    
		(468, 471), # 'QC206'    
                (472, 475), # 'QC207'   
		(476, 481), # 'DGFT'     
		(482, 485), # 'DGM'      
		(486, 489), # 'DGH'      
		(490, 494), # 'LRSE'     
		(495, 499), # 'LRFT'     
		(500, 507), # 'ARRIVEP'  
		(508, 515), # 'DEPARTP'  
		(516, 522), # 'DWELLP'   
		(523, 527), # 'NRSE'     
		(528, 533), # 'NRFT'     
		(534, 536), # 'SC'       
		(537, 543), # 'T_MILE'   
		(544, 547)] # 'CARS'


    # the column name for each variable
    COLNAMES=[  'SEQ'       ,   # (  0,   5) - stop sequence
		'V2'        ,   # (  6,  10) - not used
		'QSTOP'     ,   # ( 10,  14) - unique stop no	
		'STOPNAME'  ,   # ( 15,  47) - stop name
		'TIMESTOP_INT', # ( 48,  54) - arrival time
		'ON'        ,   # ( 55,  58) - on 
		'OFF'       ,   # ( 59,  62) - off
		'LOAD_DEP'  ,   # ( 63,  66) - departing load
		'LOADCODE'  ,   # ( 67,  67) - ADJ=*, BAL=B
		'DATE_INT'  ,   # ( 68,  74) - date
		'ROUTE'     ,   # ( 75,  79)
		'PATTERN'   ,   # ( 80,  86) - schedule pattern
		'BLOCK'     ,   # ( 87,  93) 
		'LAT'       ,   # ( 94, 102) - latitude
		'LON'       ,   # (103, 112) - longitude 
		'MILES'     ,   # (113, 118) - odometer reading (miles)
		'TRIP'      ,   # (119, 123) - trip
		'DOORCYCLES',   # (124, 125) - door cycles
		'DELTA'     ,   # (126, 130) - delta
		'DOW'       ,   # (131, 132) - day of week
		'DIR'       ,   # (133, 134)
		'VEHMILES'  ,   # (135, 140) - delta vehicle miles  - miles bus travels from last stop
		'DLPMIN'    ,   # (141, 145) - delta minutes
		'PASSMILES' ,   # (146, 153) - delta passenger miles
		'PASSHOURS' ,   # (154, 160) - delta passenger minutes
		'VEHNO'     ,   # (161, 165) - bus number
		'LINE'      ,   # (166, 170) - route (APC numeric code)
		'DBNN'      ,   # (171, 175) - data batch
		'TIMESTOP_S_INT',# (176, 180) - schedule time
		'RUNTIME_S' ,   # (181, 186) - schedule run time, in decimal minutes
		'RUNTIME'   ,   # (187, 192) - runtime from the last schedule point--TIMESTOP - DOORCLOSE of previous time point. (Excludes DWELL at the time points.), in decimal minutes
		'ODOM'      ,   # (193, 198) - not used
		'GODOM'     ,   # (199, 204) - distance (GPS)
		'TIMESTOP_DEV', # (205, 211) - schedule deviation
		'DWELL'     ,   # (212, 217) - dwell time interval (decimal minutes) -- (DOORCLOSE - TIMESTOP)
		'MSFILE'    ,   # (218, 226) - sign up YYMM
		'QC101'     ,   # (227, 230) - not used
		'QC104'     ,   # (231, 234) - GPS QC
		'QC201'     ,   # (235, 238) - count QC
		'AQC'       ,   # (239, 242) - assignment QC
		'RECORD'    ,   # (243, 244) - record type
		'WHEELCHAIR',   # (245, 246) - wheelchair
		'BIKERACK'  ,   # (247, 248) - bike rack
		'SP2'       ,   # (249, 250) - not used
		'V51'       ,   # (251, 257) - not used
		'VERSN'     ,   # (258, 263) - import version
		'DOORCLOSE_INT',# (264, 270) - departure time
		'UON'       ,   # (271, 274) - unadjusted on
		'UOFF'      ,   # (275, 278) - unadjusted off
		'CAPACITY'  ,   # (279, 283) - capacity
		'OVER'      ,   # (284, 288) - 5 over cap
		'NS'        ,   # (289, 290) - north/south
		'EW'        ,   # (291, 292) - east/west
		'MAXVEL'    ,   # (293, 296) - max velocity on previous link
		'RDBRDNGS'  ,   # (297, 300) - rear door boardings
		'DV'        ,   # (301, 304) - division
		'PATTCODE'  ,   # (305, 315) - pattern code
		'DWDI'      ,   # (316, 320) - distance traveled durign dwell
		'RUN'       ,   # (321, 328) - run
		'SCHOOL'    ,   # (329, 335) - school trip
		'TRIPID_2'  ,   # (336, 344) - long trip ID
		'PULLOUT_INT',  # (345, 351) - movement time
		'DOORCLOSE_S_INT',# (352, 356) - scheduled departure time
		'DOORCLOSE_DEV',# (357, 363) - schedule deviation
		'DWELL_S'   ,   # (364, 368) - scheduled dwell time
		'RECOVERY_S',   # (369, 374) - scheduled EOL recovery
		'RECOVERY'  ,   # (375, 380) 
		'POLITICAL' ,   # (381, 390) - not used
		'DELTAA'    ,   # (391, 397) - distance from stop at arrival
		'DELTAD'    ,   # (398, 404) - distance from stop at departure
		'ECNT'      ,   # (405, 409) - error count
		'MC'        ,   # (410, 412) - municipal code
		'DIV'       ,   # (413, 416) - division
		'LASTTRIP'  ,   # (417, 421) - previous trip
		'NEXTTRIP'  ,   # (422, 426) - next trip
		'V86'       ,   # (427, 430) - not used
		'TRIPID_3'  ,   # (431, 441)
		'WCC'       ,   # (442, 445)
		'BRC'       ,   # (446, 449)
		'DWELLI'    ,   # (450, 455)
		'QC202'     ,   # (456, 459)
		'QC302'     ,   # (460, 463)
		'QC303'     ,   # (464, 467)
		'QC206'     ,   # (468, 471)
	        'QC207'     ,   # (472, 475)
		'DGFT'      ,   # (476, 481)
		'DGM'       ,   # (482, 485)
		'DGH'       ,   # (486, 489)
		'LRSE'      ,   # (490, 494)
		'LRFT'      ,   # (495, 499)
		'ARRIVEP'   ,   # (500, 507)
		'DEPARTP'   ,   # (508, 515)
		'DWELLP'    ,   # (516, 522)
		'NRSE'      ,   # (523, 527)
		'NRFT'      ,   # (528, 533)
		'SC'        ,   # (534, 536)
		'T_MILE'    ,   # (537, 543)
		'CARS']         # (544, 547)

    # set the order of the columns in the resulting dataframe
    REORDERED_COLUMNS=[  
                # index attributes
		'DATE'      ,   # ( 68,  74) - date
		'MONTH'     ,   #            - year and month
                'DOW'       ,   #            - day of week (1=Monday, 7=Sunday)
		
		# index attributes
		'ROUTE'     ,   # ( 75,  79)
		'PATTCODE'  ,   # (305, 315) - pattern code
		'DIR'       ,   #            - direction, 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
		'TRIP'      ,   # (119, 123) - trip 
                'SEQ'       ,   # (  0,   5) - stop sequence
                
                # route/trip attributes
		'ROUTEA'    ,   #            - alphanumeric route name
		'VEHNO'     ,   # (161, 165) - bus number
		'SCHOOL'    ,   # (329, 335) - school trip
		'LASTTRIP'  ,   # (417, 421) - previous trip
		'NEXTTRIP'  ,   # (422, 426) - next trip
		'TEPPER'    ,   #            - aggregate time period
		
		# stop attributes
		'QSTOP'     ,   # ( 10,  14) - unique stop no	
		'STOPNAME'  ,   # ( 15,  47) - stop name	
		'TIMEPOINT' ,   #            - flag indicating a schedule time point
		'EOL'       ,   #            - end-of-line flag	
		
		# location information
		'LAT'       ,   # ( 94, 102) - latitude
		'LON'       ,   # (103, 112) - longitude 
		'NS'        ,   # (289, 290) - north/south
		'EW'        ,   # (291, 292) - east/west
		'MAXVEL'    ,   # (293, 296) - max velocity on previous link
		'MILES'     ,   # (113, 118) - odometer reading (miles) - cumulative, but doesn't start at zero at beginning of route
		'GODOM'     ,   # (199, 204) - distance (GPS) - cumulative, but doesn't start at zero at beginning of route
		'VEHMILES'  ,   # (135, 140) - delta vehicle miles - miles bus travels from last stop

                # ridership
		'ON'        ,   # ( 55,  58) - on 
		'OFF'       ,   # ( 59,  62) - off
		'LOAD_ARR'  ,   #            - arriving load
		'LOAD_DEP'  ,   # ( 63,  66) - departing load
		'PASSMILES' ,   # (146, 153) - delta passenger miles - LOAD_ARR * VEHMILES
		'PASSHOURS' ,   # (154, 160) - delta passenger hours - LOAD_ARR * DLPMIN / 60 -- NOT SURE THIS IS RIGHT
		'RDBRDNGS'  ,   # (297, 300) - rear door boardings
		'LOADCODE'  ,   # ( 67,  67) - ADJ=*, BAL=B
		'CAPACITY'  ,   # (279, 283) - capacity
		'DOORCYCLES',   # (124, 125) - door cycles
		'WHEELCHAIR',   # (245, 246) - wheelchair
		'BIKERACK'  ,   # (247, 248) - bike rack

                # times
		'TIMESTOP'  ,   # ( 48,  54) - arrival time
		'TIMESTOP_S',   # (176, 180) - schedule time
		'TIMESTOP_DEV', # (205, 211) - schedule deviation (TIMESTOP - TIMESTOP_S) in decimal minutes
		'ONTIME1'   ,   #            - within 1 minute of scheduled TIMESTOP
		'ONTIME5'   ,   #            - within 5 minutes of scheduled TIMESTOP
		'DOORCLOSE' ,   # (264, 270) - departure time	
		'DOORCLOSE_S',  # (352, 356) - scheduled departure time	
		'DOORCLOSE_DEV',# (357, 363) - schedule deviation (DOORCLOSE - DOORCLOSE_S) in decimal minutes
		'DWELL'     ,   # (212, 217) - dwell time (decimal minutes) -- (DOORCLOSE - TIMESTOP), zero at first and last stop
		'DWELL_S'   ,   # (364, 368) - scheduled dwell time
		'PULLOUT'   ,   # (345, 351) - movement time
		'PULLDWELL' ,   #            - pullout dwell (time interval between door close and movement), excluding end-of-line
		'RUNTIME'   ,   # (187, 192) - runtime from the last schedule point--TIMESTOP - DOORCLOSE of previous time point. (Excludes DWELL at the time points.), in decimal minutes
		'RUNTIME_S' ,   # (181, 186) - schedule run time from the last schedule point, in decimal minutes
		'RECOVERY'  ,   # (375, 380) - EOL recovery time
		'RECOVERY_S',   # (369, 374) - scheduled EOL recovery			
		'DLPMIN'    ,   # (141, 145) - delta minutes - minutes traveled from last stop -- THIS DOESN'T SEEM TO ADD UP
		
		# quality control stuff
		'QC104'     ,   # (231, 234) - GPS QC
		'QC201'     ,   # (235, 238) - count QC
		'AQC'       ,   # (239, 242) - assignment QC
		'DWDI'      ,   # (316, 320) - distance traveled durign dwell
		'DELTAA'    ,   # (391, 397) - distance from stop at arrival
		'DELTAD'    ,   # (398, 404) - distance from stop at departure
		'DELTA'         # (126, 130) - delta
		
		# additional identifying information (exclude unless needed)
		#'RECORD'    ,   # (243, 244) - record type
		#'BLOCK'     ,   # ( 87,  93)    
		#'DBNN'      ,   # (171, 175) - data batch    
		#'TRIPID_2'  ,   # (336, 344) - long trip ID
		#'RUN'       ,   # (321, 328) - run      
		#'VERSN'     ,   # (258, 263) - import version
		#'DV'        ,   # (301, 304) - division
		#'MSFILE'    ,   # (218, 226) - sign up YYMM
		#'MC'        ,   # (410, 412) - municipal code
		#'DIV'       ,   # (413, 416) - division
		#'ECNT'      ,   # (405, 409) - error count   
		]         
		    
    # uniquely define the records
    INDEX_COLUMNS=['DATE', 'ROUTE', 'PATTCODE', 'DIR', 'TRIP','SEQ'] 

    # define string lengths (otherwise would be set by first chunk)    
    STRING_LENGTHS=[  
		('ROUTEA'   ,10),   #            - alphanumeric route name
		('PATTCODE' ,10),   # (305, 315) - pattern code
		('STOPNAME' ,32),   # ( 15,  47) - stop name	
		('NS'       , 2),   # (289, 290) - north/south		
		('EW'       , 2)    # (291, 292) - east/west
                ]
                    
    def processRawData(self, infile, outfile):
        """
        Read SFMuniData, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in "raw STP" format
        outfile - output file name in h5 format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # set up the reader
        reader = pd.read_fwf(infile, 
                         colspecs = self.COLSPECS, 
                         names    = self.COLNAMES, 
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
            
            # check for further data mis-alignments
            if (chunk['NEXTTRIP'].dtype == np.dtype('int64')):
                chunk = chunk[chunk['NEXTTRIP']!=999]
            else:
                chunk = chunk[(chunk['NEXTTRIP'].str.strip()).str.count(' ')==0]

            # only include revenue service
            # dir codes: 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
            chunk = chunk[chunk['DIR'] < 2]
    
            # filter by count QC (<=20 is default)
            chunk = chunk[chunk['QC201'] <= 20]
            
            # filter where there is no route or no stop identified
            chunk = chunk[chunk['ROUTE']>0]
            chunk = chunk[chunk['QSTOP']<9999]
            
            # calculate some basic data adjustments
            chunk['LON']      = -1 * chunk['LON']
            chunk['LOAD_ARR'] = chunk['LOAD_DEP'] - chunk['ON'] + chunk['OFF']
            
            # generate empty fields        
            chunk['TIMEPOINT'] = 0 
            chunk['EOL'] = 0
            chunk['TEPPER'] = 9
            chunk['ROUTEA'] = ''
            chunk['ONTIME1'] = np.NaN
            chunk['ONTIME5'] = np.NaN            
            
            # iterate through the rows for computed fields
            for i, row in chunk.iterrows():
                
                # identify scheduled time points
                if (chunk['TIMESTOP_S_INT'][i] < 9999): 
                    chunk['TIMEPOINT'][i] = 1
                
                    # ontime performance
                    if (chunk['TIMESTOP_DEV'][i] < 1.0): 
                        chunk['ONTIME1'][i] = 1
                    else: 
                        chunk['ONTIME1'][i] = 0
                        
                    if (chunk['TIMESTOP_DEV'][i] < 5.0): 
                        chunk['ONTIME5'][i] = 1
                    else: 
                        chunk['ONTIME5'][i] = 0
                        
                # identify end-of-line stops
                chunk['EOL'][i] = str(chunk['STOPNAME'][i]).count("- EOL")            
                
                # exclude beginning and end of line from DWELL time
                if ((chunk['EOL'][i] == 1) or (chunk['SEQ'][i] == 1)): 
                    chunk['DWELL'][i] = 0
            
                # compute TEP time periods -- need to iterate
                if (chunk['TRIP'][i] >= 300  and chunk['TRIP'][i] < 600):  
                    chunk['TEPPER'][i]=300
                elif (chunk['TRIP'][i] >= 600  and chunk['TRIP'][i] < 900):  
                    chunk['TEPPER'][i]=600
                elif (chunk['TRIP'][i] >= 900  and chunk['TRIP'][i] < 1400): 
                    chunk['TEPPER'][i]=900
                elif (chunk['TRIP'][i] >= 1400 and chunk['TRIP'][i] < 1600): 
                    chunk['TEPPER'][i]=1400
                elif (chunk['TRIP'][i] >= 1600 and chunk['TRIP'][i] < 1900): 
                    chunk['TEPPER'][i]=1600
                elif (chunk['TRIP'][i] >= 1900 and chunk['TRIP'][i] < 2200): 
                    chunk['TEPPER'][i]=1900
                elif (chunk['TRIP'][i] >= 2200 and chunk['TRIP'][i] < 9999): 
                    chunk['TEPPER'][i]=2200
                            
                # compute numeric APC route to MUNI alpha -- need to iterate
                if chunk['ROUTE'][i]==0:      chunk['ROUTEA'][i] = '0'
                elif chunk['ROUTE'][i]==1:    chunk['ROUTEA'][i] = '1'
                elif chunk['ROUTE'][i]==2:    chunk['ROUTEA'][i] = '2'
                elif chunk['ROUTE'][i]==3:    chunk['ROUTEA'][i] = '3'
                elif chunk['ROUTE'][i]==4:    chunk['ROUTEA'][i] = '4'
                elif chunk['ROUTE'][i]==5:    chunk['ROUTEA'][i] = '5'
                elif chunk['ROUTE'][i]==6:    chunk['ROUTEA'][i] = '6'
                elif chunk['ROUTE'][i]==7:    chunk['ROUTEA'][i] = '7'
                elif chunk['ROUTE'][i]==9:    chunk['ROUTEA'][i] = '9'
                elif chunk['ROUTE'][i]==10:   chunk['ROUTEA'][i] = '10'
                elif chunk['ROUTE'][i]==12:   chunk['ROUTEA'][i] = '12'
                elif chunk['ROUTE'][i]==14:   chunk['ROUTEA'][i] = '14'
                elif chunk['ROUTE'][i]==15:   chunk['ROUTEA'][i] = '15'
                elif chunk['ROUTE'][i]==17:   chunk['ROUTEA'][i] = '17'
                elif chunk['ROUTE'][i]==18:   chunk['ROUTEA'][i] = '18'
                elif chunk['ROUTE'][i]==19:   chunk['ROUTEA'][i] = '19'
                elif chunk['ROUTE'][i]==20:   chunk['ROUTEA'][i] = '20'
                elif chunk['ROUTE'][i]==21:   chunk['ROUTEA'][i] = '21'
                elif chunk['ROUTE'][i]==22:   chunk['ROUTEA'][i] = '22'
                elif chunk['ROUTE'][i]==23:   chunk['ROUTEA'][i] = '23'
                elif chunk['ROUTE'][i]==24:   chunk['ROUTEA'][i] = '24'
                elif chunk['ROUTE'][i]==26:   chunk['ROUTEA'][i] = '26'
                elif chunk['ROUTE'][i]==27:   chunk['ROUTEA'][i] = '27'
                elif chunk['ROUTE'][i]==28:   chunk['ROUTEA'][i] = '28'
                elif chunk['ROUTE'][i]==29:   chunk['ROUTEA'][i] = '29'
                elif chunk['ROUTE'][i]==30:   chunk['ROUTEA'][i] = '30'
                elif chunk['ROUTE'][i]==31:   chunk['ROUTEA'][i] = '31'
                elif chunk['ROUTE'][i]==33:   chunk['ROUTEA'][i] = '33'
                elif chunk['ROUTE'][i]==35:   chunk['ROUTEA'][i] = '35'
                elif chunk['ROUTE'][i]==36:   chunk['ROUTEA'][i] = '36'
                elif chunk['ROUTE'][i]==37:   chunk['ROUTEA'][i] = '37'
                elif chunk['ROUTE'][i]==38:   chunk['ROUTEA'][i] = '38'
                elif chunk['ROUTE'][i]==39:   chunk['ROUTEA'][i] = '39'
                elif chunk['ROUTE'][i]==41:   chunk['ROUTEA'][i] = '41'
                elif chunk['ROUTE'][i]==43:   chunk['ROUTEA'][i] = '43'
                elif chunk['ROUTE'][i]==44:   chunk['ROUTEA'][i] = '44'
                elif chunk['ROUTE'][i]==45:   chunk['ROUTEA'][i] = '45'
                elif chunk['ROUTE'][i]==47:   chunk['ROUTEA'][i] = '47'
                elif chunk['ROUTE'][i]==48:   chunk['ROUTEA'][i] = '48'
                elif chunk['ROUTE'][i]==49:   chunk['ROUTEA'][i] = '49'
                elif chunk['ROUTE'][i]==52:   chunk['ROUTEA'][i] = '52'
                elif chunk['ROUTE'][i]==53:   chunk['ROUTEA'][i] = '53'
                elif chunk['ROUTE'][i]==54:   chunk['ROUTEA'][i] = '54'
                elif chunk['ROUTE'][i]==56:   chunk['ROUTEA'][i] = '56'
                elif chunk['ROUTE'][i]==66:   chunk['ROUTEA'][i] = '66'
                elif chunk['ROUTE'][i]==67:   chunk['ROUTEA'][i] = '67'
                elif chunk['ROUTE'][i]==71:   chunk['ROUTEA'][i] = '71'
                elif chunk['ROUTE'][i]==76:   chunk['ROUTEA'][i] = '76'
                elif chunk['ROUTE'][i]==88:   chunk['ROUTEA'][i] = '88'
                elif chunk['ROUTE'][i]==89:   chunk['ROUTEA'][i] = '89'
                elif chunk['ROUTE'][i]==90:   chunk['ROUTEA'][i] = '90'
                elif chunk['ROUTE'][i]==91:   chunk['ROUTEA'][i] = '91'
                elif chunk['ROUTE'][i]==92:   chunk['ROUTEA'][i] = '92'
                elif chunk['ROUTE'][i]==108:  chunk['ROUTEA'][i] = '108'
                elif chunk['ROUTE'][i]==509:  chunk['ROUTEA'][i] = '9L (509)'
                elif chunk['ROUTE'][i]==514:  chunk['ROUTEA'][i] = '14L (514)'
                elif chunk['ROUTE'][i]==528:  chunk['ROUTEA'][i] = '28L (528)'
                elif chunk['ROUTE'][i]==538:  chunk['ROUTEA'][i] = '38L (538)'
                elif chunk['ROUTE'][i]==571:  chunk['ROUTEA'][i] = '71L (571)'
                elif chunk['ROUTE'][i]==601:  chunk['ROUTEA'][i] = 'KOWL (601)'
                elif chunk['ROUTE'][i]==602:  chunk['ROUTEA'][i] = 'LOWL (602)'
                elif chunk['ROUTE'][i]==603:  chunk['ROUTEA'][i] = 'MOWL (603)'
                elif chunk['ROUTE'][i]==604:  chunk['ROUTEA'][i] = 'NOWL (604)'
                elif chunk['ROUTE'][i]==605:  chunk['ROUTEA'][i] = 'N (605)'
                elif chunk['ROUTE'][i]==606:  chunk['ROUTEA'][i] = 'J (606)'
                elif chunk['ROUTE'][i]==607:  chunk['ROUTEA'][i] = 'F (607)'
                elif chunk['ROUTE'][i]==608:  chunk['ROUTEA'][i] = 'K (608)'
                elif chunk['ROUTE'][i]==609:  chunk['ROUTEA'][i] = 'L (609)'
                elif chunk['ROUTE'][i]==610:  chunk['ROUTEA'][i] = 'M (610)'
                elif chunk['ROUTE'][i]==611:  chunk['ROUTEA'][i] = 'S (611)'
                elif chunk['ROUTE'][i]==612:  chunk['ROUTEA'][i] = 'T (612)'
                elif chunk['ROUTE'][i]==708:  chunk['ROUTEA'][i] = '8X (708)'
                elif chunk['ROUTE'][i]==709:  chunk['ROUTEA'][i] = '9X (709)'
                elif chunk['ROUTE'][i]==714:  chunk['ROUTEA'][i] = '14X (714)'
                elif chunk['ROUTE'][i]==716:  chunk['ROUTEA'][i] = '16X (716)'
                elif chunk['ROUTE'][i]==730:  chunk['ROUTEA'][i] = '30X (730)'
                elif chunk['ROUTE'][i]==780:  chunk['ROUTEA'][i] = '80X (780)'
                elif chunk['ROUTE'][i]==781:  chunk['ROUTEA'][i] = '81X (781)'
                elif chunk['ROUTE'][i]==782:  chunk['ROUTEA'][i] = '82X (782)'
                elif chunk['ROUTE'][i]==797:  chunk['ROUTEA'][i] = 'NX (797)'
                elif chunk['ROUTE'][i]==801:  chunk['ROUTEA'][i] = '1BX (801)'
                elif chunk['ROUTE'][i]==808:  chunk['ROUTEA'][i] = '8BX (808)'
                elif chunk['ROUTE'][i]==809:  chunk['ROUTEA'][i] = '9BX (809)'
                elif chunk['ROUTE'][i]==816:  chunk['ROUTEA'][i] = '16BX (816)'
                elif chunk['ROUTE'][i]==831:  chunk['ROUTEA'][i] = '31BX (831)'
                elif chunk['ROUTE'][i]==838:  chunk['ROUTEA'][i] = '38BX (838)'
                elif chunk['ROUTE'][i]==901:  chunk['ROUTEA'][i] = '1AX (901)'
                elif chunk['ROUTE'][i]==908:  chunk['ROUTEA'][i] = '8AX (908)'
                elif chunk['ROUTE'][i]==909:  chunk['ROUTEA'][i] = '9AX (909)'
                elif chunk['ROUTE'][i]==914:  chunk['ROUTEA'][i] = '14X (914)'
                elif chunk['ROUTE'][i]==916:  chunk['ROUTEA'][i] = '16AX (916)'
                elif chunk['ROUTE'][i]==931:  chunk['ROUTEA'][i] = '31AX (931)'
                elif chunk['ROUTE'][i]==938:  chunk['ROUTEA'][i] = '38AX (938)'
            
            # convert to timedate formats
            # trick here is that the MUNI service day starts and ends at 3 am, 
            # so boardings from midnight to 3 have a service date of the day before
            chunk['DATE']        = ''
            chunk['MONTH']       = ''
            chunk['TIMESTOP']    = ''
            chunk['DOORCLOSE']   = ''
            chunk['PULLOUT']     = ''
            chunk['TIMESTOP_S']  = '0101010101'
            chunk['DOORCLOSE_S'] = '0101010101'
            chunk['PULLDWELL']   = 0.0
    
            # convert to string formats
            for i, row in chunk.iterrows():        
                chunk['DATE'][i] = "{0:0>6}".format(chunk['DATE_INT'][i])   
                
                if (chunk['TIMESTOP_INT'][i] >= 240000): 
                    chunk['TIMESTOP_INT'][i] = chunk['TIMESTOP_INT'][i] - 240000
                chunk['TIMESTOP'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['TIMESTOP_INT'][i]))         
    
                if (chunk['DOORCLOSE_INT'][i] >= 240000): 
                    chunk['DOORCLOSE_INT'][i] = chunk['DOORCLOSE_INT'][i] - 240000
                chunk['DOORCLOSE'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['DOORCLOSE_INT'][i]))
    
                if (chunk['PULLOUT_INT'][i] >= 240000): 
                    chunk['PULLOUT_INT'][i] = chunk['PULLOUT_INT'][i] - 240000
                chunk['PULLOUT'][i] = (chunk['DATE'][i] + 
                    "{0:0>6}".format(chunk['PULLOUT_INT'][i]))
                
                # schedule times only at timepoints
                if (chunk['TIMEPOINT'][i]==1): 
                    if (chunk['TIMESTOP_S_INT'][i] >= 2400): 
                        chunk['TIMESTOP_S_INT'][i] = chunk['TIMESTOP_S_INT'][i] - 2400                        
                    chunk['TIMESTOP_S'][i] = (chunk['DATE'][i] + 
                        "{0:0>4}".format(chunk['TIMESTOP_S_INT'][i]))           
    
                    if (chunk['DOORCLOSE_S_INT'][i] >= 2400): 
                        chunk['DOORCLOSE_S_INT'][i] = chunk['DOORCLOSE_S_INT'][i] - 2400
                    chunk['DOORCLOSE_S'][i] = (chunk['DATE'][i] + 
                        "{0:0>4}".format(chunk['DOORCLOSE_S_INT'][i]))
    
            # convert to timedate formats
            chunk['DATE']   = pd.to_datetime(chunk['DATE'], format="%m%d%y")
            
            chunk['TIMESTOP']    = pd.to_datetime(chunk['TIMESTOP'],    format="%m%d%y%H%M%S")        
            chunk['DOORCLOSE']   = pd.to_datetime(chunk['DOORCLOSE'],   format="%m%d%y%H%M%S")    
            chunk['PULLOUT']     = pd.to_datetime(chunk['PULLOUT'],     format="%m%d%y%H%M%S")
            chunk['TIMESTOP_S']  = pd.to_datetime(chunk['TIMESTOP_S'],  format="%m%d%y%H%M")        
            chunk['DOORCLOSE_S'] = pd.to_datetime(chunk['DOORCLOSE_S'], format="%m%d%y%H%M")    

    
            # deal with offsets for midnight to 3 am
            for i, row in chunk.iterrows():       
                if (chunk['TIMESTOP'][i].hour < 3): 
                    chunk['TIMESTOP'][i] = chunk['TIMESTOP'][i] + pd.DateOffset(days=1)
    
                if (chunk['DOORCLOSE'][i].hour < 3): 
                    chunk['DOORCLOSE'][i] = chunk['DOORCLOSE'][i] + pd.DateOffset(days=1)
    
                if (chunk['PULLOUT'][i].hour < 3): 
                    chunk['PULLOUT'][i]   = chunk['PULLOUT'][i] + pd.DateOffset(days=1)
                
                # schedule only valide at timepoints
                if (chunk['TIMEPOINT'][i] == 0): 
    
                    chunk['TIMESTOP_S'][i]    = pd.NaT
                    chunk['DOORCLOSE_S'][i]   = pd.NaT
                    chunk['TIMESTOP_DEV'][i]  = np.NaN
                    chunk['DOORCLOSE_DEV'][i] = np.NaN
                    chunk['RUNTIME'][i]       = np.NaN
                    chunk['RUNTIME_S'][i]     = np.NaN
    
                else:  
                    # offsets
                    if (chunk['TIMESTOP_S'][i].hour < 3): 
                        chunk['TIMESTOP_S'][i] = chunk['TIMESTOP_S'][i] + pd.DateOffset(days=1)
    
                    if (chunk['DOORCLOSE_S'][i].hour < 3): 
                        chunk['DOORCLOSE_S'][i] = chunk['DOORCLOSE_S'][i] + pd.DateOffset(days=1)
                                              
                    
            
                # PULLDWELL = pullout dwell (time interval between door close and movement)
                if (chunk['EOL'][i]==0):
                    pulldwell = chunk['PULLOUT'][i] - chunk['DOORCLOSE'][i]
                    chunk['PULLDWELL'][i] = round(pulldwell.seconds / 60.0, 2)
                    
                # to make it easier to look up dates    
                chunk['MONTH'][i] = ((chunk['DATE'][i]).to_period('month')).to_timestamp()
                chunk['DOW'][i]   = (chunk['DATE'][i]).isoweekday()    
                              
            # because of misalinged row, it sometimes auto-detects inconsistent
            # data types, so force them as needed
            chunk['NEXTTRIP']      = chunk['NEXTTRIP'].astype('int64')
            chunk['DOORCLOSE_DEV'] = chunk['DOORCLOSE_DEV'].astype('float64')
            chunk['DELTAA']        = chunk['DELTAA'].astype('int64')   
            chunk['DELTAD']        = chunk['DELTAD'].astype('int64')   
            chunk['DWELL_S']       = chunk['DWELL_S'].astype('int64')  
            chunk['DWDI']          = chunk['DWDI'].astype('int64')
            

            # drop duplicates (not sure why these occur) and sort
            chunk.drop_duplicates(cols=self.INDEX_COLUMNS, inplace=True) 
            chunk.sort(self.INDEX_COLUMNS, inplace=True)
                            
            # re-order the columns
            df = chunk[self.REORDERED_COLUMNS]
        
            # write the data
            try: 
                store.append('df', df, data_columns=True, 
                    min_itemsize=dict(self.STRING_LENGTHS))
            except ValueError: 
                store = pd.HDFStore(outfile)
                print 'Structure of HDF5 file is: '
                print store.df.dtypes
                store.close()
                
                print 'Structure of current dataframe is: '
                print df.dtypes
                
                raise
                

            rowsWritten += len(df)
            print 'Read %i rows and kept %i rows.' % (rowsRead, rowsWritten)
            
        # close the writer
        store.close()
    

    def calcMonthlyAverages(self, hdffile, outkey):
        """
        Calculates monthly averages.  The counting equipment is only on about
        25% of the busses, so we need to average across multiple days (in this
        case the whole month) to account for all of the trips made on each route.
        
        hdffile - HDF5 file to aggregate
        outkey  - one of: 'weekday', 'saturday', or 'sunday'
                  This determines both the name of the dataframe written to the
                  HDFStore, and also the days selected for averaging. 

        notes: 
        'df'    - key for input disaggregate dataframe in h5store, as a string
                           
        """        
       
        
        # define how each field will be aggregated
        aggregationMethod = {
		'ROUTEA'       : {'ROUTEA'        : 'first'},          # route/trip attributes
		'VEHNO'        : {'VEHNO'         : 'first'},   
		'SCHOOL'       : {'SCHOOL'        : 'first'},   
		'LASTTRIP'     : {'LASTTRIP'      : 'first'},   
		'NEXTTRIP'     : {'NEXTTRIP'      : 'first'},   
		'TEPPER'       : {'TEPPER'        : 'first'},   
		'QSTOP'        : {'QSTOP'         : 'first'},          # stop attributes
		'STOPNAME'     : {'STOPNAME'      : 'first'},   
		'TIMEPOINT'    : {'TIMEPOINT'     : 'first'},   
		'EOL'          : {'EOL'           : 'first'},   
		'LAT'          : {'LAT'           : 'mean',   'LAT_STD'           :'std'},    # location information
		'LON'          : {'LON'           : 'mean',   'LON_STD'           :'std'},   
		'NS'           : {'NS'            : 'first'},       
		'EW'           : {'EW'            : 'first'},       
		'MAXVEL'       : {'MAXVEL'        : 'mean',   'MAXVEL_STD'        :'std'},   
		'MILES'        : {'MILES'         : 'mean',   'MILES_STD'         :'std'},   
		'GODOM'        : {'GODOM'         : 'mean',   'GODOM_STD'         :'std'},   
		'VEHMILES'     : {'VEHMILES'      : 'mean',   'VEHMILES_STD'      :'std'},  
		'ON'           : {'ON'            : 'mean',   'ON_STD'            :'std'},    # ridership
		'OFF'          : {'OFF'           : 'mean',   'OFF_STD'           :'std'},  
		'LOAD_ARR'     : {'LOAD_ARR'      : 'mean',   'LOAD_ARR_STD'      :'std'},  
		'LOAD_DEP'     : {'LOAD_DEP'      : 'mean',   'LOAD_DEP_STD'      :'std'},  
		'PASSMILES'    : {'PASSMILES'     : 'mean',   'PASSMILES_STD'     :'std'},  
		'PASSHOURS'    : {'PASSHOURS'     : 'mean',   'PASSHOURS_STD'     :'std'},  
		'RDBRDNGS'     : {'RDBRDNGS'      : 'mean',   'RDBRDNGS_STD'      :'std'},  
		'LOADCODE'     : {'LOADCODE'      : 'mean',   'LOADCODE_STD'      :'std'},  
		'CAPACITY'     : {'CAPACITY'      : 'mean',   'CAPACITY_STD'      :'std'},  
		'DOORCYCLES'   : {'DOORCYCLES'    : 'mean',   'DOORCYCLES_STD'    :'std'},  
		'WHEELCHAIR'   : {'WHEELCHAIR'    : 'mean',   'WHEELCHAIR_STD'    :'std'},  
		'BIKERACK'     : {'BIKERACK'      : 'mean',   'BIKERACK_STD'      :'std'},   
		'TIMESTOP_S'   : {'TIMESTOP_S'    : 'first'},                                  # times
		'TIMESTOP_DEV' : {'TIMESTOP_DEV'  : 'mean',   'TIMESTOP_DEV_STD'  :'std'},   
		'ONTIME1'      : {'ONTIME1'       : 'mean',   'ONTIME1_STD'       :'std'},   
		'ONTIME5'      : {'ONTIME5'       : 'mean',   'ONTIME5_STD'       :'std'},  
		'DOORCLOSE_S'  : {'DOORCLOSE_S'   : 'first'},  
		'DOORCLOSE_DEV': {'DOORCLOSE_DEV' : 'mean',   'DOORCLOSE_DEV_STD' :'std'}, 
		'DWELL'        : {'DWELL'         : 'mean',   'DWELL_STD'         :'std'},   
		'DWELL_S'      : {'DWELL_S'       : 'mean'},
		'PULLDWELL'    : {'PULLDWELL'     : 'mean',   'PULLDWELL_STD'     :'std'},   
		'RUNTIME'      : {'RUNTIME'       : 'mean',   'RUNTIME_STD'       :'std'},   
		'RUNTIME_S'    : {'RUNTIME_S'     : 'mean'},     
		'RECOVERY'     : {'RECOVERY'      : 'mean',   'RECOVERY_STD'      :'std'},    
		'RECOVERY_S'   : {'RECOVERY_S'    : 'mean'},   
		'DLPMIN'       : {'DLPMIN'        : 'mean',   'DLPMIN_STD'        :'std'}   
		}



        # define the order in the final dataframe
        aggregationOrder = [
                'MONTH'        , 
                'NUMDAYS'      , 
                'OBSTRIPS'     , 
		'ROUTEA'       , 
		'ROUTE'        , 
		'PATTCODE'     , 
		'DIR'          , 
		'TRIP'         , 
		'SEQ'          , 
		'VEHNO'        , 
		'SCHOOL'       , 
		'LASTTRIP'     , 
		'NEXTTRIP'     , 
		'TEPPER'       , 
		'QSTOP'        , 
		'STOPNAME'     , 
		'TIMEPOINT'    , 
		'EOL'          , 
		'LAT'          , 'LAT_STD', 
		'LON'          , 'LON_STD', 
		'NS'           , 
		'EW'           , 
		'MAXVEL'       , 'MAXVEL_STD',       
		'MILES'        , 'MILES_STD',        
		'GODOM'        , 'GODOM_STD',        
		'VEHMILES'     , 'VEHMILES_STD',     
		'ON'           , 'ON_STD',           
		'OFF'          , 'OFF_STD',          
		'LOAD_ARR'     , 'LOAD_ARR_STD',     
		'LOAD_DEP'     , 'LOAD_DEP_STD',     
		'PASSMILES'    , 'PASSMILES_STD',    
		'PASSHOURS'    , 'PASSHOURS_STD',    
		'RDBRDNGS'     , 'RDBRDNGS_STD',     
		'LOADCODE'     , 'LOADCODE_STD',     
		'CAPACITY'     , 'CAPACITY_STD',     
		'DOORCYCLES'   , 'DOORCYCLES_STD',   
		'WHEELCHAIR'   , 'WHEELCHAIR_STD',   
		'BIKERACK'     , 'BIKERACK_STD',     
		'TIMESTOP'     ,      
		'TIMESTOP_S'   , 
		'TIMESTOP_DEV' , 'TIMESTOP_DEV_STD', 
		'ONTIME1'      , 'ONTIME1_STD', 
		'ONTIME5'      , 'ONTIME5_STD', 
		'DOORCLOSE'    ,  
		'DOORCLOSE_S'  ,  
		'DOORCLOSE_DEV', 'DOORCLOSE_DEV_STD',
		'DWELL'        , 'DWELL_STD',        
		'DWELL_S'      ,      
		'PULLOUT'      ,     
		'PULLDWELL'    , 'PULLDWELL_STD',    
		'RUNTIME'      , 'RUNTIME_STD',      
		'RUNTIME_S'    ,     
		'RECOVERY'     , 'RECOVERY_STD',     
		'RECOVERY_S'   ,    
		'DLPMIN'       , 'DLPMIN_STD'       
		]

        # open and initialize the store
        store = pd.HDFStore(hdffile)
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey
        
        # get the list of all months in data set
        months = store.select_column('df', 'MONTH').unique()
        months.sort()
        print 'Retrieved a total of %i months to process' % len(months)

        # loop through the dates, and aggregate each individually
        for month in months: 
            print 'Processing ', month            

            # define the query for this part of week
            if outkey=='weekday2':
                query = 'MONTH==Timestamp(month) & DOW<=5'
            if outkey=='saturday':
                query = 'MONTH==Timestamp(month) & DOW==6'
            if outkey=='sunday':
                query = 'MONTH==Timestamp(month) & DOW==7'        
        
            df = store.select('df', where=query)
            
            # group
            grouped = df.groupby(['ROUTE', 'PATTCODE', 'DIR', 'TRIP', 'SEQ'])
            aggregated = grouped.aggregate(aggregationMethod)
            
            # drop multi-level columns
            levels = aggregated.columns.levels
            labels = aggregated.columns.labels
            aggregated.columns = levels[1][labels[1]]

            # additional calculations
            aggregated['MONTH']    = month
            aggregated['NUMDAYS']  = len(df['DATE'].unique())
            aggregated['OBSTRIPS'] = grouped.size()
            
            aggregated['TIMESTOP']  = ''
            aggregated['DOORCLOSE'] = ''
            aggregated['PULLOUT']   = ''

            for i, row in aggregated.iterrows(): 
                if aggregated['TIMEPOINT'][i]==1:
                    aggregated['TIMESTOP'][i] = (aggregated['TIMESTOP_S'][i] + 
                        pd.DateOffset(minutes=aggregated['TIMESTOP_DEV'][i]))

                    aggregated['DOORCLOSE'][i] = (aggregated['DOORCLOSE_S'][i] + 
                        pd.DateOffset(minutes=aggregated['DOORCLOSE_DEV'][i]))

                    aggregated['PULLOUT'][i] = (aggregated['DOORCLOSE'][i] + 
                        pd.DateOffset(minutes=aggregated['PULLDWELL'][i]))
            
            # force column types as needed
            aggregated['LAT']           = aggregated['LAT'].astype('float64')             
            aggregated['LON']           = aggregated['LON'].astype('float64')           
            aggregated['MAXVEL']        = aggregated['MAXVEL'].astype('float64')        
            aggregated['MILES']         = aggregated['MILES'].astype('float64')         
            aggregated['GODOM']         = aggregated['GODOM'].astype('float64')         
            aggregated['VEHMILES']      = aggregated['VEHMILES'].astype('float64')      
            aggregated['ON']            = aggregated['ON'].astype('float64')            
            aggregated['OFF']           = aggregated['OFF'].astype('float64')           
            aggregated['LOAD_ARR']      = aggregated['LOAD_ARR'].astype('float64')      
            aggregated['LOAD_DEP']      = aggregated['LOAD_DEP'].astype('float64')      
            aggregated['PASSMILES']     = aggregated['PASSMILES'].astype('float64')     
            aggregated['PASSHOURS']     = aggregated['PASSHOURS'].astype('float64')     
            aggregated['RDBRDNGS']      = aggregated['RDBRDNGS'].astype('float64')      
            aggregated['LOADCODE']      = aggregated['LOADCODE'].astype('float64')      
            aggregated['CAPACITY']      = aggregated['CAPACITY'].astype('float64')      
            aggregated['DOORCYCLES']    = aggregated['DOORCYCLES'].astype('float64')    
            aggregated['WHEELCHAIR']    = aggregated['WHEELCHAIR'].astype('float64')    
            aggregated['BIKERACK']      = aggregated['BIKERACK'].astype('float64')      
            aggregated['TIMESTOP_DEV']  = aggregated['TIMESTOP_DEV'].astype('float64')   
            aggregated['ONTIME1']       = aggregated['ONTIME1'].astype('float64')  
            aggregated['ONTIME5']       = aggregated['ONTIME5'].astype('float64')  
            aggregated['DOORCLOSE_DEV'] = aggregated['DOORCLOSE_DEV'].astype('float64') 
            aggregated['DWELL']         = aggregated['DWELL'].astype('float64')         
            aggregated['DWELL_S']       = aggregated['DWELL_S'].astype('float64')       
            aggregated['PULLDWELL']     = aggregated['PULLDWELL'].astype('float64')     
            aggregated['RUNTIME']       = aggregated['RUNTIME'].astype('float64')       
            aggregated['RUNTIME_S']     = aggregated['RUNTIME_S'].astype('float64')     
            aggregated['RECOVERY']      = aggregated['RECOVERY'].astype('float64')      
            aggregated['RECOVERY_S']    = aggregated['RECOVERY_S'].astype('float64')    
            aggregated['DLPMIN']        = aggregated['DLPMIN'].astype('float64')  
            
            # clean up structure of dataframe
            aggregated = aggregated.sort_index()
            aggregated = aggregated.reset_index()     
            aggregated = aggregated[aggregationOrder]       
            
            # write
            store.append(outkey, aggregated, data_columns=True, 
                min_itemsize=dict(self.STRING_LENGTHS))
            
        store.close()


    def aggregateTrips(self, hdffile, outkey, groupby):
        """
        Read disaggregate transit records, and aggregates across trips.
        
        hdffile - HDF5 file to aggregate
        outkey  - string - key for writing the aggregated dataframe to the store
        groupby - list - columns to group the data by
        
        notes: 

        'df'    - key for input disaggregate dataframe in h5store, as a string
                           
        """

        # define the mechanism for aggregation
        aggregationMethod = [
            ('DATE'         , 'first'),
            ('TRIP'         , 'count'),            
            ('ROUTEA'       , 'first'), 
            ('DOW'          , 'first'), 
            ('QSTOP'        , 'first'), 
            ('STOPNAME'     , 'first'), 
            ('TIMEPOINT'    , 'first'), 
            ('EOL'          , 'first'), 
            ('VEHMILES'     , 'sum'), 
            ('ON'           , 'sum'),
            ('OFF'          , 'sum'), 
            ('LOAD_ARR'     , 'sum'), 
            ('LOAD_DEP'     , 'sum'), 
            ('PASSMILES'    , 'sum'), 
            ('RDBRDNGS'     , 'sum'),
            ('CAPACITY'     , 'sum'), 
            ('DOORCYCLES'   , 'sum'),
            ('WHEELCHAIR'   , 'sum'),
            ('BIKERACK'     , 'sum'),
            ('TIMESTOP_DEV' , 'mean'), 
            ('DOORCLOSE_DEV', 'mean'), 
            ('DWELL'        , 'mean'), 
            ('DWELL_S'      , 'mean'), 
            ('PULLDWELL'    , 'mean'), 
            ('RUNTIME'      , 'mean'), 
            ('RUNTIME_S'    , 'mean'), 
            ('RECOVERY'     , 'mean'), 
            ('RECOVERY_S'   , 'mean'), 
            ('DLPMIN'       , 'mean')
            ]
            
        aggregationOrder = [
            'DATE', 
            'NUMTRIPS', 
            'ROUTEA', 
            'DOW', 
            'QSTOP', 
            'STOPNAME', 
            'TIMEPOINT', 
            'EOL', 
            'VEHMILES', 
            'ON',
            'OFF', 
            'LOAD_ARR', 
            'LOAD_DEP', 
            'PASSMILES', 
            'RDBRDNGS',
            'CAPACITY', 
            'DOORCYCLES',
            'WHEELCHAIR',
            'BIKERACK',
            'TIMESTOP_DEV', 
            'DOORCLOSE_DEV', 
            'DWELL', 
            'DWELL_S', 
            'PULLDWELL', 
            'RUNTIME', 
            'RUNTIME_S', 
            'RECOVERY', 
            'RECOVERY_S', 
            'DLPMIN'
            ]

        stringLengths=[  
		('ROUTEA'   ,10),   #            - alphanumeric route name
		('PATTCODE' ,10),   # (305, 315) - pattern code
		('STOPNAME' ,32)   # ( 15,  47) - stop name	
                ]

        # open and initialize the store
        store = pd.HDFStore(hdffile)
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey
        
        # get the list of all dates in data set
        dates = store.select_column('df', 'DATE').unique()
        print 'Retrieved a total of %i dates to process' % len(dates)

        # loop through the dates, and aggregate each individually
        for date in dates: 
            print 'Processing ', date            
            df = store.select('df', where='DATE==Timestamp(date)')

            # group
            grouped = df.groupby(groupby)
            aggregated = grouped.aggregate(dict(aggregationMethod))
            
            # clean-up
            aggregated['NUMTRIPS'] = aggregated['TRIP']
            aggregated = aggregated[aggregationOrder]
            aggregated = aggregated.sort_index()
            aggregated = aggregated.reset_index()            
            
            # write
            store.append(outkey, aggregated, data_columns=True,  
                min_itemsize=dict(stringLengths))
            
        store.close()


    def aggregateStops(self, hdffile, outkey, groupby):
        """
        Read disaggregate transit records, and aggregates across stops.
        
        hdffile - HDF5 file to aggregate
        outkey  - string - key for writing the aggregated dataframe to the store
        groupby - list - columns to group the data by
        
        notes: 

        'df'    - key for input disaggregate dataframe in h5store, as a string
                           
        """

        # define the mechanism for aggregation
        aggregationMethod = [
            ('DATE'         , 'first'),
            ('QSTOP'        , 'count'),            
            ('ROUTEA'       , 'first'), 
            ('DOW'          , 'first'),           
            ('LASTTRIP'     , 'first'), 
            ('NEXTTRIP'     , 'first'), 
            ('VEHMILES'     , 'sum'), 
            ('ON'           , 'sum'),
            ('OFF'          , 'sum'), 
            ('PASSMILES'    , 'sum'), 
            ('RDBRDNGS'     , 'sum'),
            ('CAPACITY'     , 'sum'), 
            ('DOORCYCLES'   , 'sum'),
            ('WHEELCHAIR'   , 'sum'),
            ('BIKERACK'     , 'sum'),
            ('TIMESTOP_DEV' , 'sum'), 
            ('DOORCLOSE_DEV', 'sum'), 
            ('DWELL'        , 'sum'), 
            ('DWELL_S'      , 'sum'), 
            ('PULLDWELL'    , 'sum'), 
            ('RUNTIME'      , 'sum'), 
            ('RUNTIME_S'    , 'sum'), 
            ('RECOVERY'     , 'sum'), 
            ('RECOVERY_S'   , 'sum'), 
            ('DLPMIN'       , 'sum')
            ]
            
        aggregationOrder = [
            'DATE'         , 
            'NUMSTOPS'     ,            
            'ROUTEA'       , 
            'DOW'          ,          
            'LASTTRIP'     , 
            'NEXTTRIP'     , 
            'VEHMILES'     , 
            'ON'           , 
            'OFF'          , 
            'PASSMILES'    , 
            'RDBRDNGS'     , 
            'CAPACITY'     , 
            'DOORCYCLES'   , 
            'WHEELCHAIR'   , 
            'BIKERACK'     , 
            'TIMESTOP_DEV' , 
            'DOORCLOSE_DEV', 
            'DWELL'        , 
            'DWELL_S'      , 
            'PULLDWELL'    , 
            'RUNTIME'      , 
            'RUNTIME_S'    , 
            'RECOVERY'     , 
            'RECOVERY_S'   , 
            'DLPMIN'       
            ]

        stringLengths=[  
		('ROUTEA'   ,10),   #            - alphanumeric route name
		('PATTCODE' ,10)    # (305, 315) - pattern code
                ]

        # open and initialize the store
        store = pd.HDFStore(hdffile)
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey
        
        # get the list of all dates in data set
        dates = store.select_column('df', 'DATE').unique()
        print 'Retrieved a total of %i dates to process' % len(dates)

        # loop through the dates, and aggregate each individually
        for date in dates: 
            print 'Processing ', date            
            df = store.select('df', where='DATE==Timestamp(date)')

            # group
            grouped = df.groupby(groupby)
            aggregated = grouped.aggregate(dict(aggregationMethod))
            
            # clean-up
            aggregated['NUMSTOPS'] = aggregated['QSTOP']
            aggregated = aggregated[aggregationOrder]
            aggregated = aggregated.sort_index()
            aggregated = aggregated.reset_index()            
            
            # write
            store.append(outkey, aggregated, data_columns=True,  
                min_itemsize=dict(stringLengths))
            
        store.close()

    def aggregateStopsAndTrips(self, hdffile, outkey, groupby):
        """
        Read disaggregate transit records, and aggregates across stops and trips.
        
        hdffile - HDF5 file to aggregate
        outkey  - string - key for writing the aggregated dataframe to the store
        groupby - list - columns to group the data by
        
        notes: 

        'df'    - key for input disaggregate dataframe in h5store, as a string
                           
        """

        # define the mechanism for aggregation
        aggregationMethod = [
            ('DATE'         , 'first'),
            ('TRIP'         , 'count'),            
            ('ROUTEA'       , 'first'), 
            ('DOW'          , 'first'), 
            ('VEHMILES'     , 'sum'), 
            ('ON'           , 'sum'),
            ('OFF'          , 'sum'), 
            ('PASSMILES'    , 'sum'), 
            ('RDBRDNGS'     , 'sum'),
            ('DOORCYCLES'   , 'sum'),
            ('WHEELCHAIR'   , 'sum'),
            ('BIKERACK'     , 'sum'),
            ('TIMESTOP_DEV' , 'mean'), 
            ('DOORCLOSE_DEV', 'mean'), 
            ('DWELL'        , 'sum'), 
            ('DWELL_S'      , 'sum'), 
            ('PULLDWELL'    , 'sum'), 
            ('RUNTIME'      , 'sum'), 
            ('RUNTIME_S'    , 'sum'), 
            ('RECOVERY'     , 'sum'), 
            ('RECOVERY_S'   , 'sum'), 
            ('DLPMIN'       , 'sum')
            ]
            
        aggregationOrder = [
            'DATE', 
            'NUMTRIPS', 
            'ROUTEA', 
            'DOW', 
            'VEHMILES', 
            'ON',
            'OFF', 
            'PASSMILES', 
            'RDBRDNGS',
            'DOORCYCLES',
            'WHEELCHAIR',
            'BIKERACK',
            'TIMESTOP_DEV', 
            'DOORCLOSE_DEV', 
            'DWELL', 
            'DWELL_S', 
            'PULLDWELL', 
            'RUNTIME', 
            'RUNTIME_S', 
            'RECOVERY', 
            'RECOVERY_S', 
            'DLPMIN'
            ]

        stringLengths=[  
		('ROUTEA'   ,10)   #            - alphanumeric route name
                ]

        # open and initialize the store
        store = pd.HDFStore(hdffile)
        try: 
            store.remove(outkey)
        except KeyError: 
            print "HDFStore does not contain object ", outkey
        
        # get the list of all dates in data set
        dates = store.select_column('df', 'DATE').unique()
        print 'Retrieved a total of %i dates to process' % len(dates)

        # loop through the dates, and aggregate each individually
        for date in dates: 
            print 'Processing ', date            
            df = store.select('df', where='DATE==Timestamp(date)')

            # group
            grouped = df.groupby(groupby)
            aggregated = grouped.aggregate(dict(aggregationMethod))
            
            # clean-up
            aggregated['NUMTRIPS'] = aggregated['TRIP']
            aggregated = aggregated[aggregationOrder]
            aggregated = aggregated.sort_index()
            aggregated = aggregated.reset_index()            
            
            # write
            store.append(outkey, aggregated, data_columns=True,  
                min_itemsize=dict(stringLengths))
            
        store.close()
