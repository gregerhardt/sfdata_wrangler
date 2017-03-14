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
import os

#TODO - re-calculate LOAD_ARR and LOAD_DEP after aggregating
                                    
class SFMuniDataAggregator():
    """ 
    Deals with aggregating MUNI data to daily and monthly totals.   
    """


    def __init__(self, daily_trip_outfile=None, daily_ts_outfile=None):
        """
        Constructor.                 
        """        
        
        # count the number of rows in each table so our 
        # indices are unique
        self.pattern_tod_count  = 0
        self.pattern_day_count  = 0
        self.route_tod_count  = 0
        self.route_day_count  = 0
        self.system_tod_count = 0
        self.system_day_count = 0
            
        self.rs_tod_count     = 0
        self.rs_day_count     = 0
        self.stop_tod_count   = 0
        self.stop_day_count   = 0
        self.system_tod_count_s = 0
        self.system_day_count_s = 0
            
        
        self.daily_trip_outfile = daily_trip_outfile
        self.daily_ts_outfile = daily_ts_outfile
    
        # open the output stores if specified
        if not daily_trip_outfile==None:                     
            self.trip_outstore = pd.HDFStore(daily_trip_outfile)
            
            keys = self.trip_outstore.keys()
            
            if 'pattern_tod' in keys:
                self.pattern_tod_count = len(self.trip_outstore.select('pattern_tod'))
            if 'pattern_day' in keys:
                self.pattern_day_count = len(self.trip_outstore.select('pattern_day'))
            if 'route_tod' in keys:
                self.route_tod_count = len(self.trip_outstore.select('route_tod'))
            if 'route_day' in keys:
                self.route_day_count = len(self.trip_outstore.select('route_day'))
            if 'system_tod' in keys:
                self.system_tod_count = len(self.trip_outstore.select('system_tod'))
            if 'system_day' in keys:
                self.system_day_count = len(self.trip_outstore.select('system_day'))            

        # open the output stores if specified
        if not daily_ts_outfile==None:                     
            self.ts_outstore = pd.HDFStore(daily_ts_outfile) 
            
            if 'rs_tod' in keys:
                self.rs_tod_count = len(self.trip_outstore.select('rs_tod'))
            if 'rs_day' in keys:
                self.rs_day_count = len(self.trip_outstore.select('rs_day'))
            if 'stop_tod' in keys:
                self.stop_tod_count = len(self.trip_outstore.select('stop_tod'))
            if 'stop_day' in keys:
                self.stop_day_count = len(self.trip_outstore.select('stop_day'))
            if 'system_tod_s' in keys:
                self.system_tod_count_s = len(self.trip_outstore.select('system_tod_s'))
            if 'system_day_s' in keys:
                self.system_day_count_s = len(self.trip_outstore.select('system_day_s'))      
                   
    def close(self):
        self.trip_outstore.close()
        self.ts_outstore.close()



    def aggregateToTrips(self, df):
        """
        Aggregates the dataframe from trip_stops to trip totals. 
        
        """
                    
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        AGGREGATION_RULES = [              
                ['MONTH'             ,'MONTH'             ,'first'   ,'trip' ,'datetime64', 0],          
                ['SCHED_DATES'       ,'SCHED_DATES'       ,'first'   ,'trip' ,'object'    ,20],      
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'trip' ,'int64'     , 0],         # stats for observations
                ['TRIPS'             ,'TRIPS'             ,'max'     ,'trip' ,'int64'     , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'trip' ,'int64'     , 0], 
                ['OBSERVED'          ,'OBSERVED'          ,'max'     ,'trip' ,'int64'     , 0], 
                ['FIRST_SEQ'         ,'SEQ'               ,'min'     ,'trip' ,'int64'     , 0],         # for determining PATTERN
                ['LAST_SEQ'          ,'SEQ'               ,'max'     ,'trip' ,'int64'     , 0], 
                ['NUMSTOPS'          ,'SEQ'         ,self.countUnique,'trip' ,'int64'     , 0],                 
                ['TRIP_ID'           ,'TRIP_ID'           ,'first'   ,'trip' ,'int64'     , 0],         # trip attributes  
   	        ['SHAPE_ID'          ,'SHAPE_ID'          ,'first'   ,'trip' ,'int64'     , 0],  
   	        ['PATTCODE'          ,'PATTCODE'          ,'first'   ,'trip' ,'int64'     , 0],  
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
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'trip' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'sum'     ,'trip' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'trip' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'trip' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'trip' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'trip' ,'float64'   , 0],    
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'mean'    ,'trip' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'mean'    ,'trip' ,'float64'   , 0],                 
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
                            
        # initialize new terms
        df['TRIPS'] = 1                
                            
        # trips
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'TRIP'], 
                columnSpecs=AGGREGATION_RULES, 
                level='trip', 
                weight=None)
        aggdf.index = pd.Series(range(0,len(aggdf)))
        
        # specify the PATTERN
        aggdf['PATTERN'] = (aggdf['FIRST_SEQ'].astype(str) + 
                     '_' + aggdf['LAST_SEQ'].astype(str) + 
                     '_' + aggdf['NUMSTOPS'].astype(str))                     
    
        return aggdf

    
    def aggregateTripsToDays(self, df):
        """
        Aggregates weighted data to daily totals.  
            Does this at different levels of aggregation for:
            pattern_tod, pattern_day, route_tod, route_day, system_tod, system_day
        
        """
                    
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
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'wgtSum'  ,'system' ,'float64'   , 0], 
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
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
                    
        # patterns
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'PATTERN'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight='TOD_WEIGHT')
        aggdf.index = self.pattern_tod_count + pd.Series(range(0,len(aggdf)))
        self.trip_outstore.append('pattern_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.pattern_tod_count += len(aggdf)
    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'PATTERN'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight='DAY_WEIGHT')
        aggdf.index = self.pattern_day_count + pd.Series(range(0,len(aggdf)))
        self.trip_outstore.append('pattern_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        self.pattern_day_count += len(aggdf) 


        # routes
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight='TOD_WEIGHT')
        aggdf.index = self.route_tod_count + pd.Series(range(0,len(aggdf)))
        self.trip_outstore.append('route_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.route_tod_count += len(aggdf)
    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight='DAY_WEIGHT')
        aggdf.index = self.route_day_count + pd.Series(range(0,len(aggdf)))
        self.trip_outstore.append('route_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        self.route_day_count += len(aggdf) 
    
        # system
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight='SYSTEM_WEIGHT')      
        aggdf.index = self.system_tod_count + pd.Series(range(0,len(aggdf))) 
        self.trip_outstore.append('system_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.system_tod_count += len(aggdf)
    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID'], 
                columnSpecs=TRIP_RULES, 
                level='system', 
                weight='SYSTEM_WEIGHT')     
        aggdf.index = self.system_day_count + pd.Series(range(0,len(aggdf)))                       
        self.trip_outstore.append('system_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.system_day_count += len(aggdf)
            

    
    def aggregateTripStopsToDays(self, df):
        """
        Aggregates weighted data to daily totals.  
            Does this at different levels of aggregation for:
            rs_tod, rs_day, stop_tod, stop_day, system_tod, system_day
        
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
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'wgtSum'  ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'wgtSum'  ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
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
                ['CAPACITY'          ,'CAPACITY'          ,'sum'     ,'stop'   ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'wgtSum'  ,'system' ,'float64'   , 0]  
                ]


        # route_stops    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                columnSpecs=STOP_RULES, 
                level='route_stop', 
                weight='TOD_WEIGHT')      
        aggdf.index = self.rs_tod_count + pd.Series(range(0,len(aggdf)))
        self.ts_outstore.append('rs_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)          
        self.rs_tod_count += len(aggdf)
                                                    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                columnSpecs=STOP_RULES, 
                level='route_stop', 
                weight='DAY_WEIGHT')
        aggdf.index = self.rs_day_count + pd.Series(range(0,len(aggdf)))
        self.ts_outstore.append('rs_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.rs_day_count += len(aggdf)
    
        # stops
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID','STOP_ID'], 
                columnSpecs=STOP_RULES, 
                level='stop', 
                weight='TOD_WEIGHT')
        aggdf.index = self.stop_tod_count + pd.Series(range(0,len(aggdf)))
        self.ts_outstore.append('stop_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.stop_tod_count += len(aggdf)
    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID','STOP_ID'], 
                columnSpecs=STOP_RULES, 
                level='stop', 
                weight='DAY_WEIGHT')
        aggdf.index = self.stop_day_count + pd.Series(range(0,len(aggdf)))
        self.ts_outstore.append('stop_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        self.stop_day_count += len(aggdf)

        # system
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','TOD','AGENCY_ID'], 
                columnSpecs=STOP_RULES, 
                level='system', 
                weight='SYSTEM_WEIGHT')      
        aggdf.index = self.system_tod_count_s + pd.Series(range(0,len(aggdf))) 
        self.ts_outstore.append('system_tod_s', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.system_tod_count_s += len(aggdf)
    
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['DATE','DOW','AGENCY_ID'], 
                columnSpecs=STOP_RULES, 
                level='system', 
                weight='SYSTEM_WEIGHT')     
        aggdf.index = self.system_day_count_s + pd.Series(range(0,len(aggdf)))                       
        self.ts_outstore.append('system_day_s', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        self.system_day_count_s += len(aggdf)
                        

    
    def aggregateTripsToMonths(self, daily_file, monthly_file):
        """
        Aggregates daily data to monthly totals for an average weekday/
        saturday/sunday.  Does this at different levels of aggregation for:
            pattern_tod, pattern_day, route_tod, route_day, system_tod, system_day
        
        These are unweighted, because we've already applied weights when
        calculating the daily totals. 
        """
        
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        TRIP_RULES = [              
                ['NUMDAYS'           ,'DATE'        ,self.countUnique,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBS_TRIPS'   ,np.count_nonzero,'system' ,'int64'     , 0],                        
                ['TRIPS'             ,'TRIPS'             ,'mean'    ,'system' ,'int64'     , 0],         #  note: attributes from schedule/gtfs should be unweighted             
                ['OBS_TRIPS'         ,'OBS_TRIPS'         ,'mean'    ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'WGT_TRIPS'         ,'mean'    ,'system' ,'float64'   , 0],  
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
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'mean'    ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'mean'    ,'system' ,'float64'   , 0],                 
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

        print 'Aggregating trips to month'        
        
        # delete the output file if it already exists
        if os.path.isfile(monthly_file):
            print 'Deleting previous aggregate output'
            os.remove(monthly_file)                         
        outstore = pd.HDFStore(monthly_file)
        
        # count the number of rows in each table so our 
        # indices are unique
        pattern_tod_count  = 0
        pattern_day_count  = 0
        route_tod_count  = 0
        route_day_count  = 0
        system_tod_count = 0
        system_day_count = 0

                
        # open the output file
        instore = pd.HDFStore(daily_file)                      
    
        # patterns
        print 'Processing patterns by tod'                
        df = instore.select('pattern_tod')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'PATTERN'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight=None)      
        aggdf.index = pattern_tod_count + pd.Series(range(0,len(aggdf)))

        outstore.append('pattern_tod', aggdf, data_columns=True, 
                min_itemsize=stringLengths)   
        pattern_tod_count += len(aggdf)    


        print 'Processing daily patterns'                
        df = instore.select('pattern_day')                        
        df.index = pd.Series(range(0,len(df)))                     
                
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                groupby=['MONTH','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'PATTERN'], 
                columnSpecs=TRIP_RULES, 
                level='route', 
                weight=None)      
        aggdf.index = pattern_day_count + pd.Series(range(0,len(aggdf)))

        outstore.append('pattern_day', aggdf, data_columns=True, 
                min_itemsize=stringLengths)  
        pattern_day_count += len(aggdf)     

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

    
    def aggregateTripStopsToMonths(self, daily_file, monthly_file):
        """
        Aggregates daily data to monthly totals for an average weekday/
        saturday/sunday.  Does this at different levels of aggregation for:
            rs_tod, rs_day, stop_tod, stop_day, system_tod, system_day
        
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
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'mean'    ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'mean'    ,'system' ,'float64'   , 0], 
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
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'mean'    ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'mean'    ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'mean'    ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'mean'    ,'system' ,'float64'   , 0],                 
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
                ['CAPACITY'          ,'CAPACITY'          ,'mean'    ,'stop'   ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'mean'    ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'mean'    ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'mean'    ,'system' ,'float64'   , 0]  
                ]

        print 'Aggregating trip-stops to month' 

        # delete the output file if it already exists
        if os.path.isfile(monthly_file):
            print 'Deleting previous aggregate output'
            os.remove(monthly_file)                         
        outstore = pd.HDFStore(monthly_file)
        
        # count the number of rows in each table so our 
        # indices are unique
        rs_tod_count     = 0
        rs_day_count     = 0
        stop_tod_count   = 0
        stop_day_count   = 0
        system_tod_count_s = 0
        system_day_count_s = 0

                
        # open the output file
        instore = pd.HDFStore(daily_file)
        
        # do this month-by-month to save memory
        months = instore.select_column('system_day_s', 'MONTH').unique()
        print 'Retrieved a total of %i months to process' % len(months)
        for month in months: 
            print 'Processing month ', month
        
            # route_stops
                  
            df = instore.select('rs_tod', where='MONTH=Timestamp(month)')                        
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
    
                                                               
            df = instore.select('rs_day', where='MONTH=Timestamp(month)')                        
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
            df = instore.select('stop_tod', where='MONTH=Timestamp(month)')                        
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
    
    
            df = instore.select('stop_day', where='MONTH=Timestamp(month)')                        
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
            df = instore.select('system_tod_s', where='MONTH=Timestamp(month)')                        
            df.index = pd.Series(range(0,len(df)))                     
                    
            aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','TOD','AGENCY_ID'], 
                    columnSpecs=STOP_RULES, 
                    level='system', 
                    weight=None)           
            aggdf.index = system_tod_count_s + pd.Series(range(0,len(aggdf))) 
    
            outstore.append('system_tod_s', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)   
            system_tod_count_s += len(aggdf)    
    
          
            df = instore.select('system_day_s', where='MONTH=Timestamp(month)')                        
            df.index = pd.Series(range(0,len(df)))                     
                    
            aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','AGENCY_ID'], 
                    columnSpecs=STOP_RULES, 
                    level='system', 
                    weight=None)        
            aggdf.index = system_day_count_s + pd.Series(range(0,len(aggdf)))  
                        
            outstore.append('system_day_s', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)   
            system_day_count_s += len(aggdf)    

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
        if 'RUNSPEED_S' in colorder: 
            speedInput = pd.Series(zip(aggregated['SERVMILES_S'], 
                                    aggregated['RUNTIME_S']), 
                                index=aggregated.index)     
            aggregated['RUNSPEED_S'] = speedInput.apply(self.updateSpeeds)
        
        # update actual speed--based on scheduled service miles for consistency
        if 'RUNSPEED' in colorder: 
            speedInput = pd.Series(zip(aggregated['SERVMILES'], 
                                    aggregated['RUNTIME']), 
                                index=aggregated.index)            
            aggregated['RUNSPEED'] = speedInput.apply(self.updateSpeeds)
        
        # update scheduled speed
        if 'TOTSPEED_S' in colorder: 
            speedInput = pd.Series(zip(aggregated['SERVMILES_S'], 
                                    aggregated['TOTTIME_S']), 
                                index=aggregated.index)     
            aggregated['TOTSPEED_S'] = speedInput.apply(self.updateSpeeds)
        
        # update actual speed--based on scheduled service miles for consistency
        if 'TOTSPEED' in colorder: 
            speedInput = pd.Series(zip(aggregated['SERVMILES'], 
                                    aggregated['TOTTIME']), 
                                index=aggregated.index)            
            aggregated['TOTSPEED'] = speedInput.apply(self.updateSpeeds)

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
        