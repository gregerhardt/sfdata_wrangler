
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
            if 'route_tod_tot' in keys:
                self.route_tod_count = len(self.trip_outstore.select('route_tod_tot'))
            if 'route_day' in keys:
                self.route_day_count = len(self.trip_outstore.select('route_day'))
            if 'route_day_tot' in keys:
                self.route_day_count = len(self.trip_outstore.select('route_day_tot'))
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

    
    def aggregateTripStopsByTimeOfDay(self, df):
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
                ['STOPNAME'          ,'STOPNAME'          ,'first'   ,'stop'   ,'object'    ,64],         # stop attributes
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
                ['OBSDAYS'           ,'OBS_TRIP_STOPS',np.count_nonzero,'system' ,'int64'     , 0],        
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'mean'    ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'mean'    ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'mean'    ,'system' ,'float64'   , 0], 
                ['STOP_ID'           ,'STOP_ID'           ,'first'   ,'route_stop','int64'  , 0],        
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route_stop','object' ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route_stop','int64'  , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route_stop','object' ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'mean'    ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'mean'    ,'system' ,'float64'   , 0],    
                ['STOPNAME'          ,'STOPNAME'          ,'first'   ,'stop'   ,'object'    ,64],         # stop attributes
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

        print('Aggregating trip-stops to month') 

        # establish the output file      
        outstore = pd.HDFStore(monthly_file)
        
        # count the number of rows in each table so our 
        # indices are unique
        rs_tod_count     = 0

                
        # open the output file
        instore = pd.HDFStore(daily_file)
        
        # do this month-by-month to save memory
        months = instore.select_column('rs_tod', 'MONTH').unique()
        print('Retrieved a total of %i months to process' % len(months))
        for month in months: 
            print('Processing month ', month)
        
            # route_stops
                  
            df = instore.select('rs_tod', where='MONTH=Timestamp(month)')                        
            df.index = pd.Series(range(0,len(df)))                   
                    
            aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                    columnSpecs=STOP_RULES, 
                    level='route_stop', 
                    weight=None)      
            aggdf.index = rs_tod_count + pd.Series(range(0,len(aggdf)))
    
            outstore.append('rs_tod_observed_only', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)          
            rs_tod_count += len(aggdf)
    
        instore.close()
        outstore.close()

        
    def imputeMissingTripStops(self, monthly_file):
        """
        Sometimes, there are no observed trips in a time period for 
        the whole month.  When that happens, impute the values by taking
        the matching value from the previous month. 
        """
        
        stringLengths =  {'AGENCY_ID'       : 10,  
                          'TOD'             : 10,    
                          'ROUTE_SHORT_NAME': 32, 
                          'ROUTE_LONG_NAME' : 32,          
                          'TRIP_HEADSIGN'   : 64,   
                          'STOPNAME'        : 64,         
                          'STOPNAME_AVL'    : 32
                          }
        
        impute_cols = ['TIMEPOINT', 
                       'ARRIVAL_TIME_DEV',    
                       'DEPARTURE_TIME_DEV',  
                       'DWELL',   
                       'RUNTIME', 
                       'TOTTIME', 
                       'SERVMILES',   
                       'RUNSPEED',    
                       'TOTSPEED',    
                       'ONTIME5',  
                       'ON',      
                       'OFF',    
                       'LOAD_ARR',    
                       'LOAD_DEP',   
                       'PASSMILES',   
                       'PASSHOURS',   
                       'WAITHOURS',   
                       'FULLFARE_REV',    
                       'PASSDELAY_DEP',   
                       'PASSDELAY_ARR',   
                       'RDBRDNGS',    
                       'DOORCYCLES',  
                       'WHEELCHAIR',  
                       'BIKERACK',    
                       'CAPACITY',    
                       'VC',  
                       'CROWDED', 
                       'CROWDHOURS'
                       ]
        
        # open the output file
        store = pd.HDFStore(monthly_file)
        
        keys = store.keys()
        if '/rs_tod' in keys: 
            store.remove('rs_tod')
            
        # do this month-by-month to match properly
        months = sorted(store.select_column('rs_tod_observed_only', 'MONTH').unique())
        print('Imputing missing data for %i months' % len(months))
        
        prev_month = pd.to_datetime('1900-01-01')
        for month in months: 
            print('Processing month ', month)
        
            # get current data
            df = store.select('rs_tod_observed_only', where='MONTH=Timestamp(month)')  
            
            # set imputed trip stops and colums to keep
            df['IMP_TRIP_STOPS'] = 0.
            cols = df.columns
            
            # so we can skip first month and missing months
            if prev_month in months: 
                df_prev = store.select('rs_tod', where='MONTH=Timestamp(prev_month)')  
                
                # match
                df = pd.merge(df, df_prev, 
                              how='left', 
                              on=['DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                              suffixes=['', '_PREV'], 
                              sort=True) 
                
                # fill missing values
                for col in impute_cols: 
                    df[col] = np.where(df['OBS_TRIP_STOPS']==0, df[col+'_PREV'], df[col])
                
                # make sure we know what is imputed
                df['IMP_TRIP_STOPS'] = np.where(df['OBS_TRIP_STOPS']==0, df['OBS_TRIP_STOPS_PREV'] + df['IMP_TRIP_STOPS_PREV'], 0)
                                
            # write the processed data and increment
            df = df[cols]
            store.append('rs_tod', df, data_columns=True, 
                    min_itemsize=stringLengths)
            
            prev_month = month
    
        store.close()
    
        
    def aggregateMonthlyTripStops(self, monthly_file):
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
                ['NUMDAYS'           ,'NUMDAYS'           ,'max'     ,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBSDAYS'           ,'wgtAvg'  ,'system' ,'float64'   , 0],      
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],              
                ['IMP_TRIP_STOPS'    ,'IMP_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'sum'     ,'system' ,'float64'   , 0], 
                ['STOP_ID'           ,'STOP_ID'           ,'first'   ,'route_stop','int64'  , 0],        
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route_stop','object' ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route_stop','int64'  , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route_stop','object' ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['STOPNAME'          ,'STOPNAME'          ,'first'   ,'stop'   ,'object'    ,64],         # stop attributes
                ['STOPNAME_AVL'      ,'STOPNAME_AVL'      ,'first'   ,'stop'   ,'object'    ,32],  
                ['STOP_LAT'          ,'STOP_LAT'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['STOP_LON'          ,'STOP_LON'          ,'first'   ,'stop'   ,'float64'   , 0],   
                ['EOL'               ,'EOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['SOL'               ,'SOL'               ,'first'   ,'stop'   ,'int64'     , 0],   
                ['TIMEPOINT'         ,'TIMEPOINT'         ,'first'   ,'stop'   ,'int64'     , 0],     
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'stop'   ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'stop'   ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'sum'     ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'sum'     ,'system' ,'float64'   , 0],    
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'sum'     ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'sum'     ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'sum'     ,'system' ,'float64'   , 0],   
                ['LOAD_ARR'          ,'LOAD_ARR'          ,'sum'     ,'stop'   ,'float64'   , 0],   
                ['LOAD_DEP'          ,'LOAD_DEP'          ,'sum'     ,'stop'   ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'sum'     ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'sum'     ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'sum'     ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'sum'     ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'sum'     ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'sum'     ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'sum'     ,'stop'   ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'sum'     ,'system' ,'float64'   , 0]  
                ]

        print('Aggregating route stops by TOD to daily and stop totals') 

        # establish the output file      
        store = pd.HDFStore(monthly_file)
        
        # remove the tables to be replaced
        keys = store.keys()
        if '/rs_day' in keys: 
            store.remove('rs_day')
        if '/stop_tod' in keys: 
            store.remove('stop_tod')
        if '/stop_day' in keys: 
            store.remove('stop_day')
        
        # get the data--route stop by TOD
        df = store.select('rs_tod')                        
        df.index = pd.Series(range(0,len(df)))      
        
        # daily route stops
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR', 'SEQ'], 
                    columnSpecs=STOP_RULES, 
                    level='route_stop', 
                    weight='TRIP_STOPS')
    
        store.append('rs_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
        
        # stops by time-of-day
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','TOD','AGENCY_ID','STOP_ID'], 
                    columnSpecs=STOP_RULES, 
                    level='stop', 
                    weight='TRIP_STOPS') 
    
        store.append('stop_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)          
        
        # daily stops
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','AGENCY_ID','STOP_ID'], 
                    columnSpecs=STOP_RULES, 
                    level='stop', 
                    weight='TRIP_STOPS')
    
        store.append('stop_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)          
    
        store.close()
    
    
    def aggregateMonthlyTrips(self, monthly_ts_file, monthly_trip_file):
        
        self.aggregateMonthlyRouteStopsToRoutes(monthly_ts_file, monthly_trip_file)
        self.aggregateMonthlyRoutesToTotals(monthly_trip_file)
        
    
    def aggregateMonthlyRouteStopsToRoutes(self, monthly_ts_file, monthly_trip_file):
        
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        TRIP_RULES = [              
                ['NUMDAYS'           ,'NUMDAYS'           ,'max'     ,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBSDAYS'           ,'wgtAvg'  ,'system' ,'float64'   , 0],           
                ['TRIPS'             ,'TRIP_STOPS'        ,'max'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIPS'         ,'OBS_TRIP_STOPS'    ,'max'     ,'system' ,'int64'     , 0],           
                ['IMP_TRIPS'         ,'IMP_TRIP_STOPS'    ,'max'     ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'WGT_TRIP_STOPS'    ,'max'     ,'system' ,'float64'   , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],                
                ['IMP_TRIP_STOPS'    ,'IMP_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'sum'     ,'system' ,'float64'   , 0],      
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route'  ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route'  ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route'  ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'system'   ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'system'   ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'sum'     ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'sum'     ,'system' ,'float64'   , 0],    
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'sum'     ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'sum'     ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'sum'     ,'system' ,'float64'   , 0],   
                ['MAX_LOAD'          ,'LOAD_ARR'          ,'max'     ,'route'   ,'float64'   , 0],         
                ['PASSMILES'         ,'PASSMILES'         ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'sum'     ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'sum'     ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'sum'     ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'sum'     ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'sum'     ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'sum'     ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'wgtAvg'  ,'route'   ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'max'     ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'max'     ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'sum'     ,'system' ,'float64'   , 0]  
                ]

        print('Aggregating route stops to routes') 

        # establish the output file      
        instore = pd.HDFStore(monthly_ts_file)
        outstore = pd.HDFStore(monthly_trip_file)
        
        # remove the tables to be replaced
        keys = outstore.keys()
        if '/route_dir_tod' in keys: 
            outstore.remove('route_dir_tod')
        
        # get the data--route stop by TOD
        df = instore.select('rs_tod')                        
        df.index = pd.Series(range(0,len(df)))      
        
        # patterns by time-of-day
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                    columnSpecs=TRIP_RULES, 
                    level='route', 
                    weight='TRIP_STOPS')
    
        outstore.append('route_dir_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
        
    
        instore.close()
        outstore.close()
    
    
    
    def aggregateMonthlyRoutesToTotals(self, monthly_trip_file):
        
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        TRIP_RULES = [              
                ['NUMDAYS'           ,'NUMDAYS'           ,'max'     ,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBSDAYS'           ,'wgtAvg'  ,'system' ,'float64'   , 0],           
                ['TRIPS'             ,'TRIPS'             ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIPS'         ,'OBS_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],              
                ['IMP_TRIPS'         ,'IMP_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'WGT_TRIPS'         ,'sum'     ,'system' ,'float64'   , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],                   
                ['IMP_TRIP_STOPS'    ,'IMP_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'sum'     ,'system' ,'float64'   , 0],      
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route','object' ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route','int64'  , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route','object' ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'system'   ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'system'   ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'sum'     ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'sum'     ,'system' ,'float64'   , 0],    
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'sum'     ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'sum'     ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'sum'     ,'system' ,'float64'   , 0],   
                ['MAX_LOAD'          ,'MAX_LOAD'          ,'sum'     ,'route'  ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'sum'     ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'sum'     ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'sum'     ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'sum'     ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'sum'     ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'sum'     ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'sum'     ,'route'   ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'sum'     ,'system' ,'float64'   , 0]  
                ]

        print('Aggregating routes to days') 

        # establish the output file      
        store = pd.HDFStore(monthly_trip_file)
        
        # remove the tables to be replaced
        keys = store.keys()
        if '/route_dir_day' in keys: 
            store.remove('route_dir_day')
        if '/route_tod' in keys: 
            store.remove('route_tod')
        if '/route_day' in keys: 
            store.remove('route_day')
        if '/system_tod' in keys: 
            store.remove('system_tod')
        if '/system_day' in keys: 
            store.remove('system_day')
        
        # get the data--route stop by TOD
        df = store.select('route_dir_tod')                        
        df.index = pd.Series(range(0,len(df)))      
        
        # routes by day and direction
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'AGENCY_ID','ROUTE_SHORT_NAME', 'DIR'], 
                    columnSpecs=TRIP_RULES, 
                    level='route', 
                    weight='TRIPS')
    
        store.append('route_dir_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
                    
        # routes by time-of-day 
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'TOD','AGENCY_ID','ROUTE_SHORT_NAME'], 
                    columnSpecs=TRIP_RULES, 
                    level='route', 
                    weight='TRIPS')
    
        store.append('route_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)                        
        
        # routes by day 
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'AGENCY_ID','ROUTE_SHORT_NAME'], 
                    columnSpecs=TRIP_RULES, 
                    level='route', 
                    weight='TRIPS')
    
        store.append('route_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
    
        # system by time-of-day 
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'TOD','AGENCY_ID'], 
                    columnSpecs=TRIP_RULES, 
                    level='system', 
                    weight='TRIPS')
    
        store.append('system_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)                        
        
        # system by day 
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'AGENCY_ID'], 
                    columnSpecs=TRIP_RULES, 
                    level='system', 
                    weight='TRIPS')
    
        store.append('system_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
                    
        store.close()
    
    
    def aggregateMonthlySystemTotals(self, monthly_trip_file, route_equiv_file):
        
        print('Aggregating routes to master routes and system totals') 

        # establish the output file      
        store = pd.HDFStore(monthly_trip_file)
        
        # remove the tables to be replaced
        keys = store.keys()
        if '/master_route_tod' in keys: 
            store.remove('master_route_tod')
        if '/master_route_day' in keys: 
            store.remove('master_route_day')
        if '/system_tod' in keys: 
            store.remove('system_tod')
        if '/system_day' in keys: 
            store.remove('system_day')
        
        # keep only the relevant fields in the route equivalency
        route_equiv = pd.read_csv(route_equiv_file)
        route_equiv = route_equiv[['AGENCY_ID', 'ROUTE_SHORT_NAME', 'MASTER_ROUTE_NAME']]
        
        # master-routes deal with a problem where some routes change names mid-month
        # The 5L and the 5R are a good example of this when they switch in April 2015
        # Since we've aggregated routes to monthly totals, we would double-count the riderhip
        # if we neglect to account for this.  
        

        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        MASTER_ROUTE_RULES = [              
                ['NUMDAYS'           ,'NUMDAYS'           ,'sum'     ,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBSDAYS'           ,'sum'     ,'system' ,'float64'   , 0],           
                ['TRIPS'             ,'TRIPS'             ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIPS'         ,'OBS_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],              
                ['IMP_TRIPS'         ,'IMP_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'WGT_TRIPS'         ,'wgtAvg'  ,'system' ,'float64'   , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'wgtAvg'  ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'wgtAvg'  ,'system' ,'int64'     , 0],                   
                ['IMP_TRIP_STOPS'    ,'IMP_TRIP_STOPS'    ,'wgtAvg'  ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'wgtAvg'  ,'system' ,'float64'   , 0],      
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route'  ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route'  ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route'  ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'system' ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'system' ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'wgtAvg'  ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['MAX_LOAD'          ,'MAX_LOAD'          ,'wgtAvg'  ,'route'  ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'wgtAvg'  ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'wgtAvg'  ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'wgtAvg'  ,'route'   ,'float64'  , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'wgtAvg'  ,'system' ,'float64'   , 0]  
                ]

        
        # master-routes by time-of-day 
        df = store.select('route_tod')                        
        df.index = pd.Series(range(0,len(df)))    
        df = df.merge(route_equiv, how='left', on=['AGENCY_ID', 'ROUTE_SHORT_NAME'])
        
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'TOD','AGENCY_ID','MASTER_ROUTE_NAME'], 
                    columnSpecs=MASTER_ROUTE_RULES, 
                    level='route', 
                    weight='NUMDAYS')
    
        # The 9X changes to the 8X in Dec 2009 and we're missing the data--fill that in        
        tods = aggdf['TOD'].unique()
        for tod in tods: 
            dec_idx = aggdf.index[(aggdf['MASTER_ROUTE_NAME']=='8') & (aggdf['TOD']==tod) & (aggdf['MONTH']=='2009-12-01')]
            jan_idx = aggdf.index[(aggdf['MASTER_ROUTE_NAME']=='8') & (aggdf['TOD']==tod) & (aggdf['MONTH']=='2010-01-01')]
            for col in aggdf.select_dtypes(include=['number']).columns:
                if np.isnan(aggdf.loc[dec_idx[0],col]):
                    aggdf.loc[dec_idx[0],col] = aggdf.loc[jan_idx[0],col]                
                    
        store.append('master_route_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)                        
        
        
        # master-routes by day 
        df = store.select('route_day')                        
        df.index = pd.Series(range(0,len(df)))  
        df = df.merge(route_equiv, how='left', on=['AGENCY_ID', 'ROUTE_SHORT_NAME'])
        
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'AGENCY_ID','MASTER_ROUTE_NAME'], 
                    columnSpecs=MASTER_ROUTE_RULES, 
                    level='route', 
                    weight='NUMDAYS')
    
        # The 9X changes to the 8X in Dec 2009 and we're missing the data--fill that in
        dec_idx = aggdf.index[(aggdf['MASTER_ROUTE_NAME']=='8') & (aggdf['MONTH']=='2009-12-01')]
        jan_idx = aggdf.index[(aggdf['MASTER_ROUTE_NAME']=='8') & (aggdf['MONTH']=='2010-01-01')]
        for col in aggdf.select_dtypes(include=['number']).columns:
            if np.isnan(aggdf.loc[dec_idx[0],col]):
                aggdf.loc[dec_idx[0],col] = aggdf.loc[jan_idx[0],col]
    
                
        store.append('master_route_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
    
    
        # specify 'none' as aggregation method if we want to include the 
        #   output field, but it is calculated separately
        #        outfield,            infield,  aggregationMethod,   maxlevel, type, stringLength                
        SYSTEM_RULES = [              
                ['NUMDAYS'           ,'NUMDAYS'           ,'max'     ,'system' ,'int64'     , 0],         # stats for observations
                ['OBSDAYS'           ,'OBSDAYS'           ,'wgtAvg'  ,'system' ,'float64'   , 0],           
                ['TRIPS'             ,'TRIPS'             ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIPS'         ,'OBS_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],              
                ['IMP_TRIPS'         ,'IMP_TRIPS'         ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIPS'         ,'WGT_TRIPS'         ,'sum'     ,'system' ,'float64'   , 0], 
                ['TRIP_STOPS'        ,'TRIP_STOPS'        ,'sum'     ,'system' ,'int64'     , 0],                    
                ['OBS_TRIP_STOPS'    ,'OBS_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],                   
                ['IMP_TRIP_STOPS'    ,'IMP_TRIP_STOPS'    ,'sum'     ,'system' ,'int64'     , 0],
                ['WGT_TRIP_STOPS'    ,'WGT_TRIP_STOPS'    ,'sum'     ,'system' ,'float64'   , 0],      
                ['ROUTE_LONG_NAME'   ,'ROUTE_LONG_NAME'   ,'first'   ,'route'  ,'object'    ,32],         # route attributes    
                ['ROUTE_TYPE'        ,'ROUTE_TYPE'        ,'first'   ,'route'  ,'int64'     , 0], 
                ['TRIP_HEADSIGN'     ,'TRIP_HEADSIGN'     ,'first'   ,'route'  ,'object'    ,64],   
                ['HEADWAY_S'         ,'HEADWAY_S'         ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['FARE'              ,'FARE'              ,'wgtAvg'  ,'system' ,'float64'   , 0],    
                ['ARRIVAL_TIME_DEV'  ,'ARRIVAL_TIME_DEV'  ,'wgtAvg'  ,'system' ,'float64'   , 0],         # times 
                ['DEPARTURE_TIME_DEV','DEPARTURE_TIME_DEV','wgtAvg'  ,'system' ,'float64'   , 0],   
                ['DWELL_S'           ,'DWELL_S'           ,'sum'     ,'system' ,'float64'   , 0],
                ['DWELL'             ,'DWELL'             ,'sum'     ,'system' ,'float64'   , 0],    
                ['RUNTIME_S'         ,'RUNTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNTIME'           ,'RUNTIME'           ,'sum'     ,'system' ,'float64'   , 0],    
                ['TOTTIME_S'         ,'TOTTIME_S'         ,'sum'     ,'system' ,'float64'   , 0],
                ['TOTTIME'           ,'TOTTIME'           ,'sum'     ,'system' ,'float64'   , 0],   
                ['SERVMILES_S'       ,'SERVMILES_S'       ,'sum'     ,'system' ,'float64'   , 0],
                ['SERVMILES'         ,'SERVMILES'         ,'sum'     ,'system' ,'float64'   , 0],
                ['RUNSPEED_S'        ,'RUNSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['RUNSPEED'          ,'RUNSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],  
                ['TOTSPEED_S'        ,'TOTSPEED_S'        ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['TOTSPEED'          ,'TOTSPEED'          ,'wgtAvg'  ,'system' ,'float64'   , 0],                 
                ['ONTIME5'           ,'ONTIME5'           ,'wgtAvg'  ,'system' ,'float64'   , 0],              
                ['ON'                ,'ON'                ,'sum'     ,'system' ,'float64'   , 0],         # ridership   
                ['OFF'               ,'OFF'               ,'sum'     ,'system' ,'float64'   , 0],   
                ['MAX_LOAD'          ,'MAX_LOAD'          ,'sum'     ,'route'  ,'float64'   , 0],            
                ['PASSMILES'         ,'PASSMILES'         ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSHOURS'         ,'PASSHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['WAITHOURS'         ,'WAITHOURS'         ,'sum'     ,'system' ,'float64'   , 0],  
                ['FULLFARE_REV'      ,'FULLFARE_REV'      ,'sum'     ,'system' ,'float64'   , 0],               
                ['PASSDELAY_DEP'     ,'PASSDELAY_DEP'     ,'sum'     ,'system' ,'float64'   , 0],   
                ['PASSDELAY_ARR'     ,'PASSDELAY_ARR'     ,'sum'     ,'system' ,'float64'   , 0],  
                ['RDBRDNGS'          ,'RDBRDNGS'          ,'sum'     ,'system' ,'float64'   , 0],     
                ['DOORCYCLES'        ,'DOORCYCLES'        ,'sum'     ,'system' ,'float64'   , 0],   
                ['WHEELCHAIR'        ,'WHEELCHAIR'        ,'sum'     ,'system' ,'float64'   , 0],  
                ['BIKERACK'          ,'BIKERACK'          ,'sum'     ,'system' ,'float64'   , 0],   
                ['CAPACITY'          ,'CAPACITY'          ,'sum'     ,'route'  ,'float64'   , 0],        # crowding 
                ['VC'                ,'VC'                ,'wgtAvg'  ,'system' ,'float64'   , 0],
                ['CROWDED'           ,'CROWDED'           ,'wgtAvg'  ,'system' ,'float64'   , 0],   
                ['CROWDHOURS'        ,'CROWDHOURS'        ,'sum'     ,'system' ,'float64'   , 0]  
                ]

        # system by time-of-day 
        df = store.select('master_route_tod')                        
        df.index = pd.Series(range(0,len(df)))   
        
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'TOD','AGENCY_ID'], 
                    columnSpecs=SYSTEM_RULES, 
                    level='system', 
                    weight='TRIPS')
    
        store.append('system_tod', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)                        
        
        # system by day 
        df = store.select('master_route_day')                        
        df.index = pd.Series(range(0,len(df)))   
        
        aggdf, stringLengths  = self.aggregateTransitRecords(df, 
                    groupby=['MONTH','DOW', 'AGENCY_ID'], 
                    columnSpecs=SYSTEM_RULES, 
                    level='system', 
                    weight='TRIPS')
    
        store.append('system_day', aggdf, data_columns=True, 
                    min_itemsize=stringLengths)    
                    
        store.close()
    
    
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
            if weight in aggMethod: 
                aggMethod[weight][weight] = 'sum'
            else:
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
        