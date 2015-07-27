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

import os

import pandas as pd
import datetime

    
class TransitReporter():
    """ 
    Class to create transit performance reports and associated
    visuals and graphs. 
    """

    def __init__(self, trip_file, ts_file):
        '''
        Constructor. 

        '''   
        self.trip_file = trip_file
        self.ts_file = ts_file


    """
    def assemblePerformanceData(self, outfile): 
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        
        # delete the output file if it already exists
        if os.path.isfile(outfile):
            print 'Deleting previous reporting output'
            os.remove(outfile)                         
        outstore = pd.HDFStore(outfile)


        # route data by time-of-day
        joinFields = ['MONTH','DOW','TOD','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR']
        df = self.assembleRoutePerformanceData(joinFields)
        outstore.append('route_tod', df, data_columns=True)   

        # route data by day
        joinFields = ['MONTH','DOW','AGENCY_ID','ROUTE_SHORT_NAME', 'DIR']
        df = self.assembleRoutePerformanceData(joinFields)
        outstore.append('route_day', df, data_columns=True)   


        # system data by time-of-day
        joinFields = ['MONTH','DOW','TOD','AGENCY_ID']
        df = self.assembleSystemPerformanceData(joinFields)
        outstore.append('system_tod', df, data_columns=True)   

        # system data by day
        joinFields = ['MONTH','DOW','AGENCY_ID']
        df = self.assembleSystemPerformanceData(joinFields)
        outstore.append('system_day', df, data_columns=True)   
        
        outstore.close()
    """

    """
    def assembleRoutePerformanceData(self, joinFields):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        trip_store = pd.HDFStore(self.trip_file)
        
        if 'TOD' in joinFields: 
            trips = trip_store.select('route_tod')     
        else:
            trips = trip_store.select('route_day')

        trip_store.close()
        
        # now keep the fields we want from each
        df = trips[joinFields].copy()
        
        df['TRIPS']          = trips['TRIPS']  
        df['SERVMILES_S']    = trips['SERVMILES_S']  
        df['ON']             = trips['ON']
        df['RDBRDNGS']       = trips['RDBRDNGS']
        df['PASSMILES']      = trips['PASSMILES']
        df['PASSHOURS']      = trips['PASSHOURS']
        df['WHEELCHAIR']     = trips['WHEELCHAIR']
        df['BIKERACK']       = trips['BIKERACK']
        df['RUNSPEED'] 	     = trips['RUNSPEED']
        df['DWELL_PER_STOP'] = trips['DWELL'] / trips['TRIP_STOPS']
        df['HEADWAY_S']      = trips['HEADWAY_S']
        df['FARE_PER_PASS']  = trips['FULLFARE_REV'] / trips['ON']
        df['MILES_PER_PASS'] = trips['PASSMILES'] / trips['ON']
        df['IVT_PER_PAS']    = trips['PASSHOURS'] / trips['ON']
        df['WAIT_PER_PAS']   = trips['WAITHOURS'] / trips['ON']
        df['ONTIME5']        = trips['ONTIME5']	
        df['DELAY_DEP_PER_PASS'] = trips['PASSDELAY_DEP'] / trips['ON']
        df['DELAY_ARR_PER_PASS'] = trips['PASSDELAY_ARR'] / trips['ON']
        df['VC']             = trips['VC']        
        df['CROWDED']        = trips['CROWDED']   
        df['CROWDHOURS']     = trips['CROWDHOURS']
        df['NUMDAYS']        = trips['NUMDAYS']
        df['OBSDAYS']        = trips['OBSDAYS']
        df['OBSERVED_PCT']   = trips['OBS_TRIPS'] / trips['TRIPS']
        df['MEASURE_ERR']    = trips['OFF'] / trips['ON'] - 1.0
        df['WEIGHT_ERR']     = trips['SERVMILES'] / trips['SERVMILES_S'] - 1.0
        
        return df
    """
        

    def assembleSystemPerformanceData(self, dow=1, tod='Daily'):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        trip_store = pd.HDFStore(self.trip_file)
        ts_store = pd.HDFStore(self.ts_file)
          
        if tod=='Daily': 
            trips = trip_store.select('system_day', where='DOW=dow')
            ts = ts_store.select('system_day_s', where='DOW=dow') 
        else:
            trips = trip_store.select('system_tod', where='DOW=dow & TOD=tod')
            ts = ts_store.select('system_tod_s', where='DOW=dow & TOD=tod')    

        trip_store.close()
        ts_store.close()
                                        
        # resample so any missing months show up as missing        
        trips = trips.set_index(pd.DatetimeIndex(trips['MONTH']))
        trips = trips.resample('M')
        trips['MONTH'] = trips.index

        ts = ts.set_index(pd.DatetimeIndex(ts['MONTH']))
        ts = ts.resample('M')
        ts['MONTH'] = ts.index
        
        # now the indices are aligned, so we can just assign
        df = ts[['MONTH']].copy()

        df['TRIPS']          = trips['TRIPS']
        df['SERVMILES_S']    = ts['SERVMILES_S']
        df['ON']             = ts['ON']
        df['RDBRDNGS']       = ts['RDBRDNGS']
        df['PASSMILES']      = ts['PASSMILES']
        df['PASSHOURS']      = ts['PASSHOURS']
        df['WHEELCHAIR']     = ts['WHEELCHAIR']
        df['BIKERACK']       = ts['BIKERACK']
        df['RUNSPEED'] 	     = ts['RUNSPEED']
        df['DWELL_PER_STOP'] = ts['DWELL'] / ts['TRIP_STOPS']
        df['HEADWAY_S']      = ts['HEADWAY_S']
        df['FARE_PER_PASS']  = ts['FULLFARE_REV'] / ts['ON']
        df['MILES_PER_PASS'] = ts['PASSMILES'] / ts['ON']
        df['IVT_PER_PAS']    = (ts['PASSHOURS'] / ts['ON']) * 60.0
        df['WAIT_PER_PAS']   = (ts['WAITHOURS'] / ts['ON']) * 60.0
        df['ONTIME5']        = ts['ONTIME5']	
        df['DELAY_DEP_PER_PASS'] = ts['PASSDELAY_DEP'] / ts['ON']
        df['DELAY_ARR_PER_PASS'] = ts['PASSDELAY_ARR'] / ts['ON']
        df['VC']             = trips['VC']        
        df['CROWDED']        = trips['CROWDED']   
        df['CROWDHOURS']     = ts['CROWDHOURS']
        df['NUMDAYS']        = ts['NUMDAYS']
        df['OBSDAYS']        = ts['OBSDAYS']
        df['OBSERVED_PCT']   = trips['OBS_TRIPS'] / trips['TRIPS']
        df['MEASURE_ERR']    = ts['OFF'] / ts['ON'] - 1.0
        df['WEIGHT_ERR']     = ts['SERVMILES'] / ts['SERVMILES_S'] - 1.0
        
        return df

        
    def writeSystemReport(self, xlsfile, 
                        geography='All Busses', dow=1, tod='Daily', comments=None):
        '''
        Writes a performance report for all months to the specified excel file.        
        '''        
        
        # some initial conversions
        dowString = 'unknown'
        if dow==1: 
            dowString='Average Weekday'
        elif dow==2: 
            dowString='Average Saturday'
        elif dow==3:
            dowString='Average Sunday/Holiday'       
        
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]
 
        # get the actual data
        df = self.assembleSystemPerformanceData(dow=dow, tod=tod) 
                
        # establish the writer        
        writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        

        # Write the month as the column headers
        selected = df[['MONTH']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=11, startcol=3, header=False, index=False)
                

        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        workbook  = writer.book
        worksheet = writer.sheets['Values']
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        int_format = workbook.add_format({'num_format': '#,##0'})
        dec_format = workbook.add_format({'num_format': '#,##0.00'})
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        
        # set the column widths
        worksheet.set_column(0, 1, 5)
        worksheet.set_column(0, 1, 5)
        worksheet.set_column(2, 2, 45)
                
        
        # write the header
        worksheet.write(1, 1, 'SFMTA Transit Performance Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, geography)
        worksheet.write(5, 2, 'Day-of-Week: ')
        worksheet.write(5, 3, dowString)
        worksheet.write(6, 2, 'Time-of-Day: ')
        worksheet.write(6, 3, tod)
        worksheet.write(7, 2, 'Report Generated on: ')
        worksheet.write(7, 3, timestring)
        worksheet.write(8, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
        
        worksheet.write(10, 3, 'Period', bold)
       
        
        # and the remaining data, with labels

        # SERVICE
        worksheet.write(12, 1, 'Service Provided', bold)
        worksheet.write(13, 2, 'Vehicle Trips')
        worksheet.write(14, 2, 'Service Miles')

        worksheet.set_row(13, None, int_format) 
        worksheet.set_row(14, None, int_format) 

        selected = df[['TRIPS', 'SERVMILES_S']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=13, startcol=3, header=False, index=False)
                        
        # RIDERSHIP
        worksheet.write(15, 1, 'Ridership', bold)     
        worksheet.write(16, 2, 'Boardings')      
        worksheet.write(17, 2, 'Rear-Door Boardings')      
        worksheet.write(18, 2, 'Passenger Miles')      
        worksheet.write(19, 2, 'Passenger Hours')      
        worksheet.write(20, 2, 'Wheelchairs Served')    
        worksheet.write(21, 2, 'Bicycles Served')  
    
        worksheet.set_row(16, None, int_format) 
        worksheet.set_row(17, None, int_format) 
        worksheet.set_row(18, None, int_format) 
        worksheet.set_row(19, None, int_format) 
        worksheet.set_row(20, None, int_format) 
        worksheet.set_row(21, None, int_format) 

        selected = df[['ON', 'RDBRDNGS', 'PASSMILES', 'PASSHOURS', 'WHEELCHAIR', 'BIKERACK']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=16, startcol=3, header=False, index=False)

        # LEVEL-OF-SERVICE
        worksheet.write(22, 1, 'Level-of-Service', bold)      
        worksheet.write(23, 2, 'Average Run Speed (mph)')      
        worksheet.write(24, 2, 'Average Dwell Time per Stop (min)')      
        worksheet.write(25, 2, 'Average Scheduled Headway (min)')      
        worksheet.write(26, 2, 'Average Full Fare ($)')      
        worksheet.write(27, 2, 'Average Distance Traveled per Passenger (mi)')      
        worksheet.write(28, 2, 'Average In-Vehicle Time per Passenger (min)')      
        worksheet.write(29, 2, 'Average Wait Time per Passenger (min)')      

        worksheet.set_row(23, None, dec_format) 
        worksheet.set_row(24, None, dec_format) 
        worksheet.set_row(25, None, dec_format) 
        worksheet.set_row(26, None, money_format) 
        worksheet.set_row(27, None, dec_format) 
        worksheet.set_row(28, None, dec_format) 
        worksheet.set_row(29, None, dec_format) 
 
        selected = df[['RUNSPEED', 'DWELL_PER_STOP', 'HEADWAY_S', 'FARE_PER_PASS', 'MILES_PER_PASS', 'IVT_PER_PAS', 'WAIT_PER_PAS']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=23, startcol=3, header=False, index=False)
        
        # RELIABILITY
        worksheet.write(30, 1, 'Reliability', bold)    
        worksheet.write(31, 2, 'Percent of Vehicles Arriving On-Time (-1 to +5 min)')       
        worksheet.write(32, 2, 'Average Waiting Delay per Passenger (min)')       
        worksheet.write(33, 2, 'Average Arrival Delay per Passenger (min)')       

        worksheet.set_row(31, None, percent_format) 
        worksheet.set_row(32, None, dec_format) 
        worksheet.set_row(33, None, dec_format) 

        selected = df[['ONTIME5', 'DELAY_DEP_PER_PASS', 'DELAY_ARR_PER_PASS']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=31, startcol=3, header=False, index=False)
        
        # CROWDING
        worksheet.write(34, 1, 'Crowding', bold)   
        worksheet.write(35, 2, 'Average Volume-Capacity Ratio')       
        worksheet.write(36, 2, 'Percent of Trips with V/C > 0.85')       
        worksheet.write(37, 2, 'Passenger Hours with V/C > 0.85')        
        selected = df[['VC', 'CROWDED', 'CROWDHOURS']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=35, startcol=3, header=False, index=False)       
        
        worksheet.set_row(35, None, dec_format) 
        worksheet.set_row(36, None, percent_format) 
        worksheet.set_row(37, None, int_format) 


        # OBSERVATIONS & ERROR
        worksheet.write(38, 1, 'Observations & Error', bold)       
        worksheet.write(39, 2, 'Number of Days')       
        worksheet.write(40, 2, 'Days with Observations')       
        worksheet.write(41, 2, 'Percent of Trips Observed')      
        worksheet.write(42, 2, 'Measurement Error (ON/OFF-1)')       
        worksheet.write(43, 2, 'Weighting Error (SERVMILES/SERVMILES_S-1)')      

        worksheet.set_row(39, None, int_format) 
        worksheet.set_row(40, None, int_format) 
        worksheet.set_row(41, None, percent_format) 
        worksheet.set_row(42, None, percent_format)
        worksheet.set_row(43, None, percent_format)

        selected = df[['NUMDAYS', 'OBSDAYS', 'OBSERVED_PCT', 'MEASURE_ERR', 'WEIGHT_ERR']]
        selected.T.to_excel(writer, sheet_name='Values', 
                            startrow=39, startcol=3, header=False, index=False)             
        
        writer.save()
        