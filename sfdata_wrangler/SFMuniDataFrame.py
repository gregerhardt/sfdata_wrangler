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

class SFMuniDataFrame():
    """ 
    Methods used to read SFMuni Automated Passenger Count (APC) and 
    Automated Vehicle Location (AVL) data into a Pandas data frame.  This
    includes definitions of the variables from the raw data, calculating
    computed fields, and some basic clean-up/quality control. 

    Logic is adapted from a .SPS file provided by SFMTA. 
    """
    
    # number of rows at top of file to skip
    HEADERROWS = 2

    # specifies columns for fixed-format text input
    COLSPECS=[  (  0,   5), # STOPA  - stop sequence
		(  6,  10), # 'V2'       - not used
		( 10,  14), # 'QSTOP'    - unique stop no	
		( 15,  47), # 'ANAME'    - stop name
		( 48,  54), # 'TIMESTOP' - arrival time
		( 55,  58), # 'ON'       - on 
		( 59,  62), # 'OFF'      - off
		( 63,  66), # 'LOAD'     - departing load
		( 67,  67), # 'LOADCODE' - ADJ=*, BAL=B
		( 68,  74), # 'DATE'     - date
		( 75,  79), # 'ROUTE'    
		( 80,  86), # 'PATTERN'  - schedule pattern
		( 87,  93), # 'BLOCK'  
		( 94, 102), # 'LAT'      - latitude
		(103, 112), # 'LON'      - longitude 
		(113, 118), # 'MILES'    
		(119, 123), # 'TRIP'     - trip
		(124, 125), # 'DRCLS'    - door cycles
		(126, 130), # 'DELTA'    - delta
		(131, 132), # 'DOW'      - day of week
		(133, 134), # 'DIR'      
		(135, 140), # 'DLMILES'  - delta miles 
		(141, 145), # 'DLPMIN'   - delta minutes
		(146, 153), # 'DLPMLS'   - delta passenger miles
		(154, 160), # 'DLPHRS'   - delta passenger minutes
		(161, 165), # 'VEHNO'    - bus number
		(166, 170), # 'LINE'     - route (APC numeric code)
		(171, 175), # 'DBNN'     - data batch
		(176, 180), # 'SCHTIM'   - schedule time
		(181, 186), # 'SRTIME'   - schedule run time
		(187, 192), # 'ARTIME'   
		(193, 198), # 'ODOM'     - not used
		(199, 204), # 'GODOM'    - distance (GPS)
		(205, 211), # 'SCHDEV'   - schedule deviation
		(212, 217), # 'DWTIME'   - dwell time interval (decimal minutes)
		(218, 226), # 'MSFILE'   - sign up YYMM
		(227, 230), # 'QC101'    - not used
		(231, 234), # 'QC104'    - GPS QC
		(235, 238), # 'QC201'    - count QC
		(239, 242), # 'AQC'      - assignment QC
		(243, 244), # 'RECORD'   - record type
		(245, 246), # 'W'        - wheelchair
		(247, 248), # 'SP1'      - bike rack
		(249, 250), # 'SP2'      - not used
		(251, 257), # 'V51'      - not used
		(258, 263), # 'VERSN'    - import version
		(264, 270), # 'DEPART'   - departure time
		(271, 274), # 'UON'      - unadjusted on
		(275, 278), # 'UOFF'     - unadjusted off
		(279, 283), # 'FULL'     - capacity
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
		(345, 351), # 'DEPARTI'  - movement time
		(352, 356), # 'SCHED'    - scheduled departure time
		(357, 363), # 'DEVIAD'   - schedule deviation
		(364, 368), # 'SCDW'     - scheduled dwell time
		(369, 374), # 'SREC'     - scheduled EOL recovery
		(375, 380), # 'AREC'     
		(381, 390), # 'POLITICAL'- not used
		(391, 397), # 'DELTAA'   - distance from stop at arrival
		(398, 404), # 'DELTAD'   - distance from stop at departure
		(405, 409), # 'ECNT'     - error count
		(410, 412), # 'MC'       - municipal code
		(413, 416), # 'DIV'      - division
		(417, 421), # 'LAST'     - previous trip
		(422, 426), # 'NEXT'     - next trip
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
    COLNAMES=[  'STOPA'     ,   # (  0,   5) - stop sequence
		'V2'        ,   # (  6,  10) - not used
		'QSTOP'     ,   # ( 10,  14) - unique stop no	
		'ANAME'     ,   # ( 15,  47) - stop name
		'TIMESTOP'  ,   # ( 48,  54) - arrival time
		'ON'        ,   # ( 55,  58) - on 
		'OFF'       ,   # ( 59,  62) - off
		'LOAD'      ,   # ( 63,  66) - departing load
		'LOADCODE'  ,   # ( 67,  67) - ADJ=*, BAL=B
		'DATE'      ,   # ( 68,  74) - date
		'ROUTE'     ,   # ( 75,  79)
		'PATTERN'   ,   # ( 80,  86) - schedule pattern
		'BLOCK'     ,   # ( 87,  93) 
		'LAT'       ,   # ( 94, 102) - latitude
		'LON'       ,   # (103, 112) - longitude 
		'MILES'     ,   # (113, 118) 
		'TRIP'      ,   # (119, 123) - trip
		'DRCLS'     ,   # (124, 125) - door cycles
		'DELTA'     ,   # (126, 130) - delta
		'DOW'       ,   # (131, 132) - day of week
		'DIR'       ,   # (133, 134)
		'DLMILES'   ,   # (135, 140) - delta miles 
		'DLPMIN'    ,   # (141, 145) - delta minutes
		'DLPMLS'    ,   # (146, 153) - delta passenger miles
		'DLPHRS'    ,   # (154, 160) - delta passenger minutes
		'VEHNO'     ,   # (161, 165) - bus number
		'LINE'      ,   # (166, 170) - route (APC numeric code)
		'DBNN'      ,   # (171, 175) - data batch
		'TIMESTOP_S',   # (176, 180) - schedule time
		'SRTIME'    ,   # (181, 186) - schedule run time
		'ARTIME'    ,   # (187, 192) 
		'ODOM'      ,   # (193, 198) - not used
		'GODOM'     ,   # (199, 204) - distance (GPS)
		'SCHDEV'    ,   # (205, 211) - schedule deviation
		'DWTIME'    ,   # (212, 217) - dwell time interval (decimal minutes)
		'MSFILE'    ,   # (218, 226) - sign up YYMM
		'QC101'     ,   # (227, 230) - not used
		'QC104'     ,   # (231, 234) - GPS QC
		'QC201'     ,   # (235, 238) - count QC
		'AQC'       ,   # (239, 242) - assignment QC
		'RECORD'    ,   # (243, 244) - record type
		'W'         ,   # (245, 246) - wheelchair
		'SP1'       ,   # (247, 248) - bike rack
		'SP2'       ,   # (249, 250) - not used
		'V51'       ,   # (251, 257) - not used
		'VERSN'     ,   # (258, 263) - import version
		'DOORCLOSE' ,   # (264, 270) - departure time
		'UON'       ,   # (271, 274) - unadjusted on
		'UOFF'      ,   # (275, 278) - unadjusted off
		'FULL'      ,   # (279, 283) - capacity
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
		'PULLOUT'   ,   # (345, 351) - movement time
		'DOORCLOSE_S',  # (352, 356) - scheduled departure time
		'DEVIAD'    ,   # (357, 363) - schedule deviation
		'SCDW'      ,   # (364, 368) - scheduled dwell time
		'SREC'      ,   # (369, 374) - scheduled EOL recovery
		'AREC'      ,   # (375, 380) 
		'POLITICAL' ,   # (381, 390) - not used
		'DELTAA'    ,   # (391, 397) - distance from stop at arrival
		'DELTAD'    ,   # (398, 404) - distance from stop at departure
		'ECNT'      ,   # (405, 409) - error count
		'MC'        ,   # (410, 412) - municipal code
		'DIV'       ,   # (413, 416) - division
		'LAST'      ,   # (417, 421) - previous trip
		'NEXT'      ,   # (422, 426) - next trip
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

    @staticmethod
    def read(filename):
        """
        Read SFMuniData and return it as a pandas dataframe
        
        filename - just that
        columnsToUse - array of column names or numbers to read
        rowsToRead - read this many rows from the file
        """
        df = pd.read_fwf(filename, 
                         colspecs = SFMuniDataFrame.COLSPECS, 
                         names    = SFMuniDataFrame.COLNAMES, 
                         skiprows = SFMuniDataFrame.HEADERROWS, 
                         usecols  = SFMuniDataFrame.COLUMNS_TO_READ, 
                         nrows    = 1000)

        # only include revenue service
        # dir codes: 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
        df = df[df['DIR'] < 2]

        # filter by count QC (<=20 is default)
        df = df[df['QC201'] <= 20]

        # calculate some basic data adjustments
        df['LON']    = -1 * df['LON']
        df['STOPA']  = 1000 * df['STOPA']
        df['RTEDIR'] = 1000 * df['ROUTE'] + 0.1 * df['DIR']
        
        # convert DATE, TIMESTOP, DOORCLOSE, PULLOUT to datetime format
        df['DATE']      = pd.to_datetime(df['DATE'],      format="%m%d%y")
        df['TIMESTOP']  = pd.to_datetime(df['TIMESTOP'],  format="%H%M%S")
        df['DOORCLOSE'] = pd.to_datetime(df['DOORCLOSE'], format="%H%M%S")
        df['PULLOUT']   = pd.to_datetime(df['PULLOUT'],   format="%H%M%S")  
        
        # same with scheduled times
        # funny stuff in formatting adds leading zeros as needed
        # seems like this needs to loop through the rows
        #df['TIMESTOP_S'] = df['TIMESTOP_S'].replace(9999, 0)
        #df['TIMESTOP_S2'] = pd.to_datetime(df['TIMESTOP_S'], format="%H%M")
        #df['TIMESTOP_S2'] = str(df['TIMESTOP_S'])
        #df['TIMESTOP_S2']  = "{0:0>4}".format(df['TIMESTOP_S'])
        #df['DOORCLOSE_S'].replace(9999, np.nan)
        #df['DOORCLOSE_S'] = pd.to_datetime("{0:0>4}".format(df['DOORCLOSE_S']), 
        #    format="%H%M")
        
        # create unique string TRIPCODE to describe each trip
        #df['TRIPCODE'] = str(df['RTEDIR']) + "_" + str(df['VEHNO']) + "_" + \
        #                 str(df['DATE']) + "_" + str(df['TRIP'])
        
        # create unique string TRIPSTOP to create sortable sequence of stops
        #df['TRIPSTOP'] = str(df['STOPA']) + "_" + df['ANAME']
        
        # create unique string RTDIRSEQ to identify trip/dir/seq/stop
        #df['RTDIRSEQ'] = str(df['RTEDIR']) + "." + str(df['STOPA']) + "." + \
        #                 str(df['QSTOP'])
        
        # DOORTIME = passenger dwell (time interval doors open)
        df['DOORDWELL'] = df['DOORCLOSE'] - df['TIMESTOP']
        
        # WAITTIME = pullout dwell (time interval between door close and movement)
        df['WAITDWELL'] = df['PULLOUT'] - df['DOORCLOSE']
        
        # exclude dwell and wait delays at terminals
        df['EOL'] = str(df['ANAME']).count("- EOL")
        df['DOORDTIME'] = df['DOORDWELL'] * df['EOL']
        df['WAITDTIME'] = df['WAITDWELL'] * df['EOL']
        
        # iterate through the rows for computed fields
        df['TEPPER'] = 9
        df['ROUTEA'] = ''
        for row_index, row in df.iterrows():
                    
            # compute TEP time periods -- need to iterate
            if (df['TRIP'][row_index] >= 300  and df['TRIP'][row_index] < 600):  
                df['TEPPER'][row_index]=300
            elif (df['TRIP'][row_index] >= 600  and df['TRIP'][row_index] < 900):  
                df['TEPPER'][row_index]=600
            elif (df['TRIP'][row_index] >= 900  and df['TRIP'][row_index] < 1400): 
                df['TEPPER'][row_index]=900
            elif (df['TRIP'][row_index] >= 1400 and df['TRIP'][row_index] < 1600): 
                df['TEPPER'][row_index]=1400
            elif (df['TRIP'][row_index] >= 1600 and df['TRIP'][row_index] < 1900): 
                df['TEPPER'][row_index]=1600
            elif (df['TRIP'][row_index] >= 1900 and df['TRIP'][row_index] < 2200): 
                df['TEPPER'][row_index]=1900
            elif (df['TRIP'][row_index] >= 2200 and df['TRIP'][row_index] < 9999): 
                df['TEPPER'][row_index]=2200
                        
            # compute numeric APC route to MUNI alpha -- need to iterate
            if df['ROUTE'][row_index]==0:      df['ROUTEA'][row_index] = '0'
            elif df['ROUTE'][row_index]==1:    df['ROUTEA'][row_index] = '1'
            elif df['ROUTE'][row_index]==2:    df['ROUTEA'][row_index] = '2'
            elif df['ROUTE'][row_index]==3:    df['ROUTEA'][row_index] = '3'
            elif df['ROUTE'][row_index]==4:    df['ROUTEA'][row_index] = '4'
            elif df['ROUTE'][row_index]==5:    df['ROUTEA'][row_index] = '5'
            elif df['ROUTE'][row_index]==6:    df['ROUTEA'][row_index] = '6'
            elif df['ROUTE'][row_index]==7:    df['ROUTEA'][row_index] = '7'
            elif df['ROUTE'][row_index]==9:    df['ROUTEA'][row_index] = '9'
            elif df['ROUTE'][row_index]==10:   df['ROUTEA'][row_index] = '10'
            elif df['ROUTE'][row_index]==12:   df['ROUTEA'][row_index] = '12'
            elif df['ROUTE'][row_index]==14:   df['ROUTEA'][row_index] = '14'
            elif df['ROUTE'][row_index]==15:   df['ROUTEA'][row_index] = '15'
            elif df['ROUTE'][row_index]==17:   df['ROUTEA'][row_index] = '17'
            elif df['ROUTE'][row_index]==18:   df['ROUTEA'][row_index] = '18'
            elif df['ROUTE'][row_index]==19:   df['ROUTEA'][row_index] = '19'
            elif df['ROUTE'][row_index]==20:   df['ROUTEA'][row_index] = '20'
            elif df['ROUTE'][row_index]==21:   df['ROUTEA'][row_index] = '21'
            elif df['ROUTE'][row_index]==22:   df['ROUTEA'][row_index] = '22'
            elif df['ROUTE'][row_index]==23:   df['ROUTEA'][row_index] = '23'
            elif df['ROUTE'][row_index]==24:   df['ROUTEA'][row_index] = '24'
            elif df['ROUTE'][row_index]==26:   df['ROUTEA'][row_index] = '26'
            elif df['ROUTE'][row_index]==27:   df['ROUTEA'][row_index] = '27'
            elif df['ROUTE'][row_index]==28:   df['ROUTEA'][row_index] = '28'
            elif df['ROUTE'][row_index]==29:   df['ROUTEA'][row_index] = '29'
            elif df['ROUTE'][row_index]==30:   df['ROUTEA'][row_index] = '30'
            elif df['ROUTE'][row_index]==31:   df['ROUTEA'][row_index] = '31'
            elif df['ROUTE'][row_index]==33:   df['ROUTEA'][row_index] = '33'
            elif df['ROUTE'][row_index]==35:   df['ROUTEA'][row_index] = '35'
            elif df['ROUTE'][row_index]==36:   df['ROUTEA'][row_index] = '36'
            elif df['ROUTE'][row_index]==37:   df['ROUTEA'][row_index] = '37'
            elif df['ROUTE'][row_index]==38:   df['ROUTEA'][row_index] = '38'
            elif df['ROUTE'][row_index]==39:   df['ROUTEA'][row_index] = '39'
            elif df['ROUTE'][row_index]==41:   df['ROUTEA'][row_index] = '41'
            elif df['ROUTE'][row_index]==43:   df['ROUTEA'][row_index] = '43'
            elif df['ROUTE'][row_index]==44:   df['ROUTEA'][row_index] = '44'
            elif df['ROUTE'][row_index]==45:   df['ROUTEA'][row_index] = '45'
            elif df['ROUTE'][row_index]==47:   df['ROUTEA'][row_index] = '47'
            elif df['ROUTE'][row_index]==48:   df['ROUTEA'][row_index] = '48'
            elif df['ROUTE'][row_index]==49:   df['ROUTEA'][row_index] = '49'
            elif df['ROUTE'][row_index]==52:   df['ROUTEA'][row_index] = '52'
            elif df['ROUTE'][row_index]==53:   df['ROUTEA'][row_index] = '53'
            elif df['ROUTE'][row_index]==54:   df['ROUTEA'][row_index] = '54'
            elif df['ROUTE'][row_index]==56:   df['ROUTEA'][row_index] = '56'
            elif df['ROUTE'][row_index]==66:   df['ROUTEA'][row_index] = '66'
            elif df['ROUTE'][row_index]==67:   df['ROUTEA'][row_index] = '67'
            elif df['ROUTE'][row_index]==71:   df['ROUTEA'][row_index] = '71'
            elif df['ROUTE'][row_index]==76:   df['ROUTEA'][row_index] = '76'
            elif df['ROUTE'][row_index]==88:   df['ROUTEA'][row_index] = '88'
            elif df['ROUTE'][row_index]==89:   df['ROUTEA'][row_index] = '89'
            elif df['ROUTE'][row_index]==90:   df['ROUTEA'][row_index] = '90'
            elif df['ROUTE'][row_index]==91:   df['ROUTEA'][row_index] = '91'
            elif df['ROUTE'][row_index]==92:   df['ROUTEA'][row_index] = '92'
            elif df['ROUTE'][row_index]==108:  df['ROUTEA'][row_index] = '108'
            elif df['ROUTE'][row_index]==509:  df['ROUTEA'][row_index] = '9L (509)'
            elif df['ROUTE'][row_index]==514:  df['ROUTEA'][row_index] = '14L (514)'
            elif df['ROUTE'][row_index]==528:  df['ROUTEA'][row_index] = '28L (528)'
            elif df['ROUTE'][row_index]==538:  df['ROUTEA'][row_index] = '38L (538)'
            elif df['ROUTE'][row_index]==571:  df['ROUTEA'][row_index] = '71L (571)'
            elif df['ROUTE'][row_index]==601:  df['ROUTEA'][row_index] = 'KOWL (601)'
            elif df['ROUTE'][row_index]==602:  df['ROUTEA'][row_index] = 'LOWL (602)'
            elif df['ROUTE'][row_index]==603:  df['ROUTEA'][row_index] = 'MOWL (603)'
            elif df['ROUTE'][row_index]==604:  df['ROUTEA'][row_index] = 'NOWL (604)'
            elif df['ROUTE'][row_index]==605:  df['ROUTEA'][row_index] = 'N (605)'
            elif df['ROUTE'][row_index]==606:  df['ROUTEA'][row_index] = 'J (606)'
            elif df['ROUTE'][row_index]==607:  df['ROUTEA'][row_index] = 'F (607)'
            elif df['ROUTE'][row_index]==608:  df['ROUTEA'][row_index] = 'K (608)'
            elif df['ROUTE'][row_index]==609:  df['ROUTEA'][row_index] = 'L (609)'
            elif df['ROUTE'][row_index]==610:  df['ROUTEA'][row_index] = 'M (610)'
            elif df['ROUTE'][row_index]==611:  df['ROUTEA'][row_index] = 'S (611)'
            elif df['ROUTE'][row_index]==612:  df['ROUTEA'][row_index] = 'T (612)'
            elif df['ROUTE'][row_index]==708:  df['ROUTEA'][row_index] = '8X (708)'
            elif df['ROUTE'][row_index]==709:  df['ROUTEA'][row_index] = '9X (709)'
            elif df['ROUTE'][row_index]==714:  df['ROUTEA'][row_index] = '14X (714)'
            elif df['ROUTE'][row_index]==716:  df['ROUTEA'][row_index] = '16X (716)'
            elif df['ROUTE'][row_index]==730:  df['ROUTEA'][row_index] = '30X (730)'
            elif df['ROUTE'][row_index]==780:  df['ROUTEA'][row_index] = '80X (780)'
            elif df['ROUTE'][row_index]==781:  df['ROUTEA'][row_index] = '81X (781)'
            elif df['ROUTE'][row_index]==782:  df['ROUTEA'][row_index] = '82X (782)'
            elif df['ROUTE'][row_index]==797:  df['ROUTEA'][row_index] = 'NX (797)'
            elif df['ROUTE'][row_index]==801:  df['ROUTEA'][row_index] = '1BX (801)'
            elif df['ROUTE'][row_index]==808:  df['ROUTEA'][row_index] = '8BX (808)'
            elif df['ROUTE'][row_index]==809:  df['ROUTEA'][row_index] = '9BX (809)'
            elif df['ROUTE'][row_index]==816:  df['ROUTEA'][row_index] = '16BX (816)'
            elif df['ROUTE'][row_index]==831:  df['ROUTEA'][row_index] = '31BX (831)'
            elif df['ROUTE'][row_index]==838:  df['ROUTEA'][row_index] = '38BX (838)'
            elif df['ROUTE'][row_index]==901:  df['ROUTEA'][row_index] = '1AX (901)'
            elif df['ROUTE'][row_index]==908:  df['ROUTEA'][row_index] = '8AX (908)'
            elif df['ROUTE'][row_index]==909:  df['ROUTEA'][row_index] = '9AX (909)'
            elif df['ROUTE'][row_index]==914:  df['ROUTEA'][row_index] = '14X (914)'
            elif df['ROUTE'][row_index]==916:  df['ROUTEA'][row_index] = '16AX (916)'
            elif df['ROUTE'][row_index]==931:  df['ROUTEA'][row_index] = '31AX (931)'
            elif df['ROUTE'][row_index]==938:  df['ROUTEA'][row_index] = '38AX (938)'
        
        
        return df
    