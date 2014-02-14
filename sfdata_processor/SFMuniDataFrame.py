# -*- coding: utf-8 -*-
__author__      = "Gregory D. Erhardt"
__copyright__   = "Copyright 2013 SFCTA"
__license__     = """
    This file is part of sfdata_processor.

    sfdata_processor is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    sfdata_processor is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with sfdata_processor.  If not, see <http://www.gnu.org/licenses/>.
"""

import pandas as pd

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
		'SCHTIM'    ,   # (176, 180) - schedule time
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
		'SCHED'     ,   # (352, 356) - scheduled departure time
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


    
    @staticmethod
    def read(filename):
        """
        Read SFMuniData and return it as a pandas dataframe
        """
        df = pd.read_fwf(filename, 
                         colspecs = SFMuniDataFrame.COLSPECS, 
                         names    = SFMuniDataFrame.COLNAMES, 
                         skiprows = SFMuniDataFrame.HEADERROWS, 
                         nrows    = 20)

        # only include revenue service
        # dir codes: 0-outbound, 1-inbound, 6-pull out, 7-pull in, 8-pull mid
        df = df[df['DIR'] < 2]

        # filter by count QC (<=20 is default)
        df = df[df['QC201'] <= 20]

        # calculate some basic data adjustments
        df['LON']    = -1 * df['LON']
        df['STOPA']  = 1000 * df['STOPA']
        df['RTEDIR'] = 1000 * df['ROUTE'] + 0.1 * df['DIR']
        
        # convert TIMESTOP, DOORCLOSE and PULLOUT to datetime format
        
        return df
    