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
import pytables

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
		(131, 132), # 'DOW'      - day of week
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
		(297, 300), # 'RDBRDNGS' - rear door boardings
		(301, 304), # 'DV'       - division
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


    # by default, read the first 75 columns, through NEXT
    COLUMNS_TO_READ = [i for i in range(75)]

    # set the order of the columns in the resulting dataframe
    REORDERED_COLUMNS=[  
                # index attributes
		'DATE'      ,   # ( 68,  74) - date
		'ROUTE'     ,   # ( 75,  79)
		'DIR'       ,   #            - direction, 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
		'TRIP'      ,   # (119, 123) - trip 
                'SEQ'       ,   # (  0,   5) - stop sequence
                
                # route/trip attributes
		'ROUTEA'    ,   #            - alphanumeric route name
		'PATTCODE'  ,   # (305, 315) - pattern code
		'VEHNO'     ,   # (161, 165) - bus number
		'SCHOOL'    ,   # (329, 335) - school trip
		'LASTTRIP'  ,   # (417, 421) - previous trip
		'NEXTTRIP'  ,   # (422, 426) - next trip
		'DOW'       ,   # (131, 132) - day of week
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
		    

    INDEX_COLUMNS=[  
		'DATE'      ,   # ( 68,  74) - date
		'ROUTE'     ,   # ( 75,  79)
		'DIR'       ,   #            - direction, 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
		'TRIP'      ,   # (119, 123) - trip
                'SEQ'       ,   # (  0,   5) - stop sequence
                ] 


    def read_stp(self, filename):
        """
        Read SFMuniData and return it as a pandas dataframe
        
        filename - in "raw STP" format
        """
        df = pd.read_fwf(filename, 
                         colspecs = self.COLSPECS, 
                         names    = self.COLNAMES, 
                         skiprows = self.HEADERROWS, 
                         usecols  = self.COLUMNS_TO_READ, 
                         iterator = True, 
                         chunksize= 10000, 
                         nrows    = 100000)

        # only include revenue service
        # dir codes: 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
        df = df[df['DIR'] < 2]

        # filter by count QC (<=20 is default)
        df = df[df['QC201'] <= 20]
        
        # filter where there is no route or no stop identified
        df = df[df['ROUTE']>0]
        df = df[df['QSTOP']<9999]
        
        # calculate some basic data adjustments
        df['LON']      = -1 * df['LON']
        df['LOAD_ARR'] = df['LOAD_DEP'] - df['ON'] + df['OFF']
        
        # generate empty fields        
        df['TIMEPOINT'] = 0 
        df['EOL'] = 0
        df['TEPPER'] = 9
        df['ROUTEA'] = ''
        
        # iterate through the rows for computed fields
        for i, row in df.iterrows():
            
            # identify scheduled time points
            if (df['TIMESTOP_S_INT'][i] < 9999): 
                df['TIMEPOINT'][i] = 1
            
            # identify end-of-line stops
            df['EOL'][i] = str(df['STOPNAME'][i]).count("- EOL")            
            
            # exclude beginning and end of line from DWELL time
            if ((df['EOL'][i] == 1) or (df['SEQ'][i] == 1)): 
                df['DWELL'][i] = 0
        
            # compute TEP time periods -- need to iterate
            if (df['TRIP'][i] >= 300  and df['TRIP'][i] < 600):  
                df['TEPPER'][i]=300
            elif (df['TRIP'][i] >= 600  and df['TRIP'][i] < 900):  
                df['TEPPER'][i]=600
            elif (df['TRIP'][i] >= 900  and df['TRIP'][i] < 1400): 
                df['TEPPER'][i]=900
            elif (df['TRIP'][i] >= 1400 and df['TRIP'][i] < 1600): 
                df['TEPPER'][i]=1400
            elif (df['TRIP'][i] >= 1600 and df['TRIP'][i] < 1900): 
                df['TEPPER'][i]=1600
            elif (df['TRIP'][i] >= 1900 and df['TRIP'][i] < 2200): 
                df['TEPPER'][i]=1900
            elif (df['TRIP'][i] >= 2200 and df['TRIP'][i] < 9999): 
                df['TEPPER'][i]=2200
                        
            # compute numeric APC route to MUNI alpha -- need to iterate
            if df['ROUTE'][i]==0:      df['ROUTEA'][i] = '0'
            elif df['ROUTE'][i]==1:    df['ROUTEA'][i] = '1'
            elif df['ROUTE'][i]==2:    df['ROUTEA'][i] = '2'
            elif df['ROUTE'][i]==3:    df['ROUTEA'][i] = '3'
            elif df['ROUTE'][i]==4:    df['ROUTEA'][i] = '4'
            elif df['ROUTE'][i]==5:    df['ROUTEA'][i] = '5'
            elif df['ROUTE'][i]==6:    df['ROUTEA'][i] = '6'
            elif df['ROUTE'][i]==7:    df['ROUTEA'][i] = '7'
            elif df['ROUTE'][i]==9:    df['ROUTEA'][i] = '9'
            elif df['ROUTE'][i]==10:   df['ROUTEA'][i] = '10'
            elif df['ROUTE'][i]==12:   df['ROUTEA'][i] = '12'
            elif df['ROUTE'][i]==14:   df['ROUTEA'][i] = '14'
            elif df['ROUTE'][i]==15:   df['ROUTEA'][i] = '15'
            elif df['ROUTE'][i]==17:   df['ROUTEA'][i] = '17'
            elif df['ROUTE'][i]==18:   df['ROUTEA'][i] = '18'
            elif df['ROUTE'][i]==19:   df['ROUTEA'][i] = '19'
            elif df['ROUTE'][i]==20:   df['ROUTEA'][i] = '20'
            elif df['ROUTE'][i]==21:   df['ROUTEA'][i] = '21'
            elif df['ROUTE'][i]==22:   df['ROUTEA'][i] = '22'
            elif df['ROUTE'][i]==23:   df['ROUTEA'][i] = '23'
            elif df['ROUTE'][i]==24:   df['ROUTEA'][i] = '24'
            elif df['ROUTE'][i]==26:   df['ROUTEA'][i] = '26'
            elif df['ROUTE'][i]==27:   df['ROUTEA'][i] = '27'
            elif df['ROUTE'][i]==28:   df['ROUTEA'][i] = '28'
            elif df['ROUTE'][i]==29:   df['ROUTEA'][i] = '29'
            elif df['ROUTE'][i]==30:   df['ROUTEA'][i] = '30'
            elif df['ROUTE'][i]==31:   df['ROUTEA'][i] = '31'
            elif df['ROUTE'][i]==33:   df['ROUTEA'][i] = '33'
            elif df['ROUTE'][i]==35:   df['ROUTEA'][i] = '35'
            elif df['ROUTE'][i]==36:   df['ROUTEA'][i] = '36'
            elif df['ROUTE'][i]==37:   df['ROUTEA'][i] = '37'
            elif df['ROUTE'][i]==38:   df['ROUTEA'][i] = '38'
            elif df['ROUTE'][i]==39:   df['ROUTEA'][i] = '39'
            elif df['ROUTE'][i]==41:   df['ROUTEA'][i] = '41'
            elif df['ROUTE'][i]==43:   df['ROUTEA'][i] = '43'
            elif df['ROUTE'][i]==44:   df['ROUTEA'][i] = '44'
            elif df['ROUTE'][i]==45:   df['ROUTEA'][i] = '45'
            elif df['ROUTE'][i]==47:   df['ROUTEA'][i] = '47'
            elif df['ROUTE'][i]==48:   df['ROUTEA'][i] = '48'
            elif df['ROUTE'][i]==49:   df['ROUTEA'][i] = '49'
            elif df['ROUTE'][i]==52:   df['ROUTEA'][i] = '52'
            elif df['ROUTE'][i]==53:   df['ROUTEA'][i] = '53'
            elif df['ROUTE'][i]==54:   df['ROUTEA'][i] = '54'
            elif df['ROUTE'][i]==56:   df['ROUTEA'][i] = '56'
            elif df['ROUTE'][i]==66:   df['ROUTEA'][i] = '66'
            elif df['ROUTE'][i]==67:   df['ROUTEA'][i] = '67'
            elif df['ROUTE'][i]==71:   df['ROUTEA'][i] = '71'
            elif df['ROUTE'][i]==76:   df['ROUTEA'][i] = '76'
            elif df['ROUTE'][i]==88:   df['ROUTEA'][i] = '88'
            elif df['ROUTE'][i]==89:   df['ROUTEA'][i] = '89'
            elif df['ROUTE'][i]==90:   df['ROUTEA'][i] = '90'
            elif df['ROUTE'][i]==91:   df['ROUTEA'][i] = '91'
            elif df['ROUTE'][i]==92:   df['ROUTEA'][i] = '92'
            elif df['ROUTE'][i]==108:  df['ROUTEA'][i] = '108'
            elif df['ROUTE'][i]==509:  df['ROUTEA'][i] = '9L (509)'
            elif df['ROUTE'][i]==514:  df['ROUTEA'][i] = '14L (514)'
            elif df['ROUTE'][i]==528:  df['ROUTEA'][i] = '28L (528)'
            elif df['ROUTE'][i]==538:  df['ROUTEA'][i] = '38L (538)'
            elif df['ROUTE'][i]==571:  df['ROUTEA'][i] = '71L (571)'
            elif df['ROUTE'][i]==601:  df['ROUTEA'][i] = 'KOWL (601)'
            elif df['ROUTE'][i]==602:  df['ROUTEA'][i] = 'LOWL (602)'
            elif df['ROUTE'][i]==603:  df['ROUTEA'][i] = 'MOWL (603)'
            elif df['ROUTE'][i]==604:  df['ROUTEA'][i] = 'NOWL (604)'
            elif df['ROUTE'][i]==605:  df['ROUTEA'][i] = 'N (605)'
            elif df['ROUTE'][i]==606:  df['ROUTEA'][i] = 'J (606)'
            elif df['ROUTE'][i]==607:  df['ROUTEA'][i] = 'F (607)'
            elif df['ROUTE'][i]==608:  df['ROUTEA'][i] = 'K (608)'
            elif df['ROUTE'][i]==609:  df['ROUTEA'][i] = 'L (609)'
            elif df['ROUTE'][i]==610:  df['ROUTEA'][i] = 'M (610)'
            elif df['ROUTE'][i]==611:  df['ROUTEA'][i] = 'S (611)'
            elif df['ROUTE'][i]==612:  df['ROUTEA'][i] = 'T (612)'
            elif df['ROUTE'][i]==708:  df['ROUTEA'][i] = '8X (708)'
            elif df['ROUTE'][i]==709:  df['ROUTEA'][i] = '9X (709)'
            elif df['ROUTE'][i]==714:  df['ROUTEA'][i] = '14X (714)'
            elif df['ROUTE'][i]==716:  df['ROUTEA'][i] = '16X (716)'
            elif df['ROUTE'][i]==730:  df['ROUTEA'][i] = '30X (730)'
            elif df['ROUTE'][i]==780:  df['ROUTEA'][i] = '80X (780)'
            elif df['ROUTE'][i]==781:  df['ROUTEA'][i] = '81X (781)'
            elif df['ROUTE'][i]==782:  df['ROUTEA'][i] = '82X (782)'
            elif df['ROUTE'][i]==797:  df['ROUTEA'][i] = 'NX (797)'
            elif df['ROUTE'][i]==801:  df['ROUTEA'][i] = '1BX (801)'
            elif df['ROUTE'][i]==808:  df['ROUTEA'][i] = '8BX (808)'
            elif df['ROUTE'][i]==809:  df['ROUTEA'][i] = '9BX (809)'
            elif df['ROUTE'][i]==816:  df['ROUTEA'][i] = '16BX (816)'
            elif df['ROUTE'][i]==831:  df['ROUTEA'][i] = '31BX (831)'
            elif df['ROUTE'][i]==838:  df['ROUTEA'][i] = '38BX (838)'
            elif df['ROUTE'][i]==901:  df['ROUTEA'][i] = '1AX (901)'
            elif df['ROUTE'][i]==908:  df['ROUTEA'][i] = '8AX (908)'
            elif df['ROUTE'][i]==909:  df['ROUTEA'][i] = '9AX (909)'
            elif df['ROUTE'][i]==914:  df['ROUTEA'][i] = '14X (914)'
            elif df['ROUTE'][i]==916:  df['ROUTEA'][i] = '16AX (916)'
            elif df['ROUTE'][i]==931:  df['ROUTEA'][i] = '31AX (931)'
            elif df['ROUTE'][i]==938:  df['ROUTEA'][i] = '38AX (938)'
        
        # convert to timedate formats
        # trick here is that the MUNI service day starts and ends at 3 am, 
        # so boardings from midnight to 3 have a service date of the day before
        df['DATE']        = ''
        df['TIMESTOP']    = ''
        df['DOORCLOSE']   = ''
        df['PULLOUT']     = ''
        df['TIMESTOP_S']  = '0101010101'
        df['DOORCLOSE_S'] = '0101010101'
        df['PULLDWELL']   = 0.0

        # convert to string formats
        for i, row in df.iterrows():        
            df['DATE'][i] = "{0:0>6}".format(df['DATE_INT'][i])   
            
            if (df['TIMESTOP_INT'][i] >= 240000): 
                df['TIMESTOP_INT'][i] = df['TIMESTOP_INT'][i] - 240000
            df['TIMESTOP'][i] = df['DATE'][i] + "{0:0>6}".format(df['TIMESTOP_INT'][i])            

            if (df['DOORCLOSE_INT'][i] >= 240000): 
                df['DOORCLOSE_INT'][i] = df['DOORCLOSE_INT'][i] - 240000
            df['DOORCLOSE'][i] = df['DATE'][i] + "{0:0>6}".format(df['DOORCLOSE_INT'][i])

            if (df['PULLOUT_INT'][i] >= 240000): 
                df['PULLOUT_INT'][i] = df['PULLOUT_INT'][i] - 240000
            df['PULLOUT'][i] = df['DATE'][i] + "{0:0>6}".format(df['PULLOUT_INT'][i])
            
            # schedule times only at timepoints
            if (df['TIMEPOINT'][i]==1): 
                if (df['TIMESTOP_S_INT'][i] >= 2400): 
                    df['TIMESTOP_S_INT'][i] = df['TIMESTOP_S_INT'][i] - 2400
                df['TIMESTOP_S'][i] = df['DATE'][i] + "{0:0>4}".format(df['TIMESTOP_S_INT'][i])            

                if (df['DOORCLOSE_S_INT'][i] >= 2400): 
                    df['DOORCLOSE_S_INT'][i] = df['DOORCLOSE_S_INT'][i] - 2400
                df['DOORCLOSE_S'][i] = df['DATE'][i] + "{0:0>4}".format(df['DOORCLOSE_S_INT'][i])

        # convert to timedate formats
        df['DATE'] = pd.to_datetime(df['DATE'], format="%m%d%y")
        df['TIMESTOP']    = pd.to_datetime(df['TIMESTOP'],    format="%m%d%y%H%M%S")        
        df['DOORCLOSE']   = pd.to_datetime(df['DOORCLOSE'],   format="%m%d%y%H%M%S")    
        df['PULLOUT']     = pd.to_datetime(df['PULLOUT'],     format="%m%d%y%H%M%S")
        df['TIMESTOP_S']  = pd.to_datetime(df['TIMESTOP_S'],  format="%m%d%y%H%M")        
        df['DOORCLOSE_S'] = pd.to_datetime(df['DOORCLOSE_S'], format="%m%d%y%H%M")    

        # deal with offsets for midnight to 3 am
        for i, row in df.iterrows():       
            if (df['TIMESTOP'][i].hour < 3): 
                df['TIMESTOP'][i] = df['TIMESTOP'][i] + pd.DateOffset(days=1)

            if (df['DOORCLOSE'][i].hour < 3): 
                df['DOORCLOSE'][i] = df['DOORCLOSE'][i] + pd.DateOffset(days=1)

            if (df['PULLOUT'][i].hour < 3): 
                df['PULLOUT'][i]   = df['PULLOUT'][i] + pd.DateOffset(days=1)
            
            # schedule only valide at timepoints
            if (df['TIMEPOINT'][i] == 0): 

                df['TIMESTOP_S'][i]    = pd.NaT
                df['DOORCLOSE_S'][i]   = pd.NaT
                df['TIMESTOP_DEV'][i]  = np.NaN
                df['DOORCLOSE_DEV'][i] = np.NaN
                df['RUNTIME'][i]       = np.NaN
                df['RUNTIME_S'][i]     = np.NaN

            else:     
                
                if (df['TIMESTOP_S'][i].hour < 3): 
                    df['TIMESTOP_S'][i] = df['TIMESTOP_S'][i] + pd.DateOffset(days=1)

                if (df['DOORCLOSE_S'][i].hour < 3): 
                    df['DOORCLOSE_S'][i] = df['DOORCLOSE_S'][i] + pd.DateOffset(days=1)
        
            # PULLDWELL = pullout dwell (time interval between door close and movement)
            if (df['EOL'][i]==0):
                pulldwell = df['PULLOUT'][i] - df['DOORCLOSE'][i]
                df['PULLDWELL'][i] = round(pulldwell.seconds / 60.0, 2)
                
        # replace missing values as appropriate -- this doesn't seem to work
        df['SCHOOL'].replace(9999, 0)
        
        # sort in logical order for viewing
        df.sort_index(by=self.INDEX_COLUMNS, inplace=True)
        
        # drop duplicates (not sure why these occur)
        df.drop_duplicates(cols=self.INDEX_COLUMNS, inplace=True) 
        
        # set the index 
        df.set_index(self.INDEX_COLUMNS, drop=False, inplace=True, 
            verify_integrity=True)

        # re-order the columns
        df2 = df[self.REORDERED_COLUMNS]

        return df2
    
    
    def write_hdf(self, df, filename):
        """
        Writes processed SFMuniData to a HDF5 file. 
        
        filename - output file to write
        """
        df.to_hdf(filename, 'table', append=False)        
    
    
    def read_hdf(self, filename):
        """
        Read SFMuniData and return it as a pandas dataframe
        
        filename - in converted hdf5 format
        """
        df  = pd.read_hdf(filename, 'table')
        
        return df