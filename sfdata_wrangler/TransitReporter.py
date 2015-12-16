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
import numpy as np
import pandas as pd
import datetime
from xlsxwriter.utility import xl_rowcol_to_cell
import bokeh.plotting as bk

    
class TransitReporter():
    """ 
    Class to create transit performance reports and associated
    visuals and graphs. 
    """

    def __init__(self, trip_file, ts_file, demand_file, gtfs_file, multimodal_file):
        '''
        Constructor. 

        '''   
        self.trip_file = trip_file
        self.ts_file = ts_file
        self.demand_file = demand_file
        self.gtfs_file = gtfs_file
        self.multimodal_file = multimodal_file

        self.writer = None
        self.worksheet = None
        self.row = None
        self.col = None


    def assembleSystemPerformanceData(self, fips, dow=1, tod='Daily'):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        trip_store = pd.HDFStore(self.trip_file)
        ts_store = pd.HDFStore(self.ts_file)
        demand_store = pd.HDFStore(self.demand_file)
          
        if tod=='Daily': 
            trips = trip_store.select('system_day', where='DOW=dow')
            ts = ts_store.select('system_day_s', where='DOW=dow') 
        else:
            trips = trip_store.select('system_tod', where='DOW=dow & TOD=tod')
            ts = ts_store.select('system_tod_s', where='DOW=dow & TOD=tod')    

        employment = demand_store.select('countyEmp', where='FIPS=fips')
        population = demand_store.select('countyPop', where='FIPS=fips')
        autoOpCost = demand_store.select('autoOpCost')

        trip_store.close()
        ts_store.close()
        demand_store.close()
                                        
        # resample so any missing months show up as missing   
        # the offsets are to get it based on the first day of the month instead of the last     
        trips = trips.set_index(pd.DatetimeIndex(trips['MONTH']))
        trips = trips.resample('M')
        trips['MONTH'] = trips.index
        trips['MONTH'] = trips['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))

        ts = ts.set_index(pd.DatetimeIndex(ts['MONTH']))
        ts = ts.resample('M')
        ts['MONTH'] = ts.index
        ts['MONTH'] = ts['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # now the indices are aligned, so we can just assign
        df = ts[['MONTH']].copy()

        df['TRIPS']          = trips['TRIPS']
        df['SERVMILES']      = ts['SERVMILES']
        df['SERVMILES_S']    = ts['SERVMILES_S']
        df['ON']             = ts['ON']
        df['RDBRDNGS']       = ts['RDBRDNGS']
        df['PASSMILES']      = ts['PASSMILES']
        df['PASSHOURS']      = ts['PASSHOURS']
        df['WHEELCHAIR']     = ts['WHEELCHAIR']
        df['BIKERACK']       = ts['BIKERACK']
        df['RUNSPEED'] 	     = ts['RUNSPEED']
        df['TOTSPEED'] 	     = ts['TOTSPEED']
        df['DWELL_PER_STOP'] = ts['DWELL'] / ts['TRIP_STOPS']
        df['HEADWAY_S']      = ts['HEADWAY_S']
        df['FARE_PER_PASS']  = ts['FULLFARE_REV'] / ts['ON']
        df['MILES_PER_PASS'] = ts['PASSMILES'] / ts['ON']
        df['IVT_PER_PAS']    = (ts['PASSHOURS'] / ts['ON']) * 60.0
        df['PASSPEED']       = (df['MILES_PER_PASS'] / df['IVT_PER_PAS']) * 60.0
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

        # additional fields for estimation
        df['OFF_MINUS_ON']   = ts['OFF'] - ts['ON']
        df['SERVMILES_MINUS_SERVMILES_S']   = ts['SERVMILES'] - ts['SERVMILES_S']
        
        df['MEASURE_ERR_ON']  = df['MEASURE_ERR'] * df['ON']
        df['WEIGHT_ERR_ON']   = df['WEIGHT_ERR'] * df['ON']
        
        
        # merge the drivers of demand data
        # employment includes TOTEMP
        df = pd.merge(df, employment, how='left', on=['MONTH'], sort=True) 
        
        # employment includes POP
        df = pd.merge(df, population, how='left', on=['MONTH'], sort=True)  
        
        # fuelPrice includes FUEL_PRICE and FUEL_PRICE_2010USD
        df = pd.merge(df, autoOpCost, how='left', on=['MONTH'], sort=True)  
        
        return df

        
    def writeSystemReport(self, xlsfile, fips, 
                        geography='All Busses', dow=1, comments=None):
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
 
        # establish the writer        
        writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        

        # write a separate sheet for each TOD
        tods = ['Daily', '0300-0559', '0600-0859', '0900-1359', 
                '1400-1559', '1600-1859', '1900-2159', '2200-0259'] 
                
        for tod in tods: 
                
            # get the actual data
            df = self.assembleSystemPerformanceData(fips=fips, dow=dow, tod=tod)    
                    
            # Write the month as the column headers
            months = df[['MONTH']]
            months.T.to_excel(writer, sheet_name=tod, 
                                startrow=11, startcol=4, header=False, index=False)
                    
    
            # note that we have to call the pandas function first to get the
            # excel sheet created properly, so now we can access that
            workbook  = writer.book
            worksheet = writer.sheets[tod]
            
            # set up the formatting, with defaults
            bold = workbook.add_format({'bold': 1})        
            
            # set the column widths
            worksheet.set_column(0, 1, 5)
            worksheet.set_column(0, 1, 5)
            worksheet.set_column(2, 2, 45)
            worksheet.set_column(3, 3, 25)
                    
            
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
            
            
            # Use formulas to calculate the differences
            self.writeSystemValues(df, writer, months, tod)
            self.writeSystemDifferenceFormulas(writer, months, tod)
            self.writeSystemPercentDifferenceFormulas(writer, months, tod)    
            
            # freeze so we can see what's happening
            worksheet.freeze_panes(0, 4)
            
        writer.save()
    
    def writeSystemValues(self, df, writer, months, tod):
        '''
        Writes the main system values to the worksheet. 
        '''
    
        # which cells to look at
        max_col = 3+len(months)+1
        
        # get the worksheet
        workbook  = writer.book
        worksheet = writer.sheets[tod]        
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        int_format = workbook.add_format({'num_format': '#,##0'})
        dec_format = workbook.add_format({'num_format': '#,##0.00'})
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # HEADER
        worksheet.write(10, 4, 'Values', bold)
        worksheet.write(11, 3, 'Trend', bold)
        
        # DRIVERS OF DEMAND
        worksheet.write(12, 1, 'Drivers of Demand', bold)
        worksheet.write(13, 2, 'Employment')
        worksheet.write(14, 2, 'Population')
        worksheet.write(15, 2, 'Average Fuel Price (2010 $)')        

        worksheet.set_row(13, None, int_format) 
        worksheet.set_row(14, None, int_format) 
        worksheet.set_row(15, None, money_format) 

        selected = df[['TOTEMP', 'POP', 'FUEL_PRICE_2010USD']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=13, startcol=4, header=False, index=False)  

        for r in range(13,16):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})   


        # SERVICE
        worksheet.write(16, 1, 'Service Provided', bold)
        worksheet.write(17, 2, 'Vehicle Trips')
        worksheet.write(18, 2, 'Service Miles')

        worksheet.set_row(17, None, int_format) 
        worksheet.set_row(18, None, int_format) 

        selected = df[['TRIPS', 'SERVMILES_S']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=17, startcol=4, header=False, index=False)     
                               
        for r in range(17,19):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
                        
        # RIDERSHIP
        worksheet.write(19, 1, 'Ridership', bold)     
        worksheet.write(20, 2, 'Boardings')      
        worksheet.write(21, 2, 'Rear-Door Boardings')      
        worksheet.write(22, 2, 'Passenger Miles')      
        worksheet.write(23, 2, 'Passenger Hours')      
        worksheet.write(24, 2, 'Wheelchairs Served')    
        worksheet.write(25, 2, 'Bicycles Served')  
    
        worksheet.set_row(20, None, int_format) 
        worksheet.set_row(21, None, int_format) 
        worksheet.set_row(22, None, int_format) 
        worksheet.set_row(23, None, int_format) 
        worksheet.set_row(24, None, int_format) 
        worksheet.set_row(25, None, int_format) 

        selected = df[['ON', 'RDBRDNGS', 'PASSMILES', 'PASSHOURS', 'WHEELCHAIR', 'BIKERACK']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=20, startcol=4, header=False, index=False)

        for r in range(20,27):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
                        
        # LEVEL-OF-SERVICE
        worksheet.write(26, 1, 'Level-of-Service', bold)      
        worksheet.write(27, 2, 'Average Run Speed (mph)')      
        worksheet.write(28, 2, 'Average Total Speed (mph)')      
        worksheet.write(29, 2, 'Average Dwell Time per Stop (min)')      
        worksheet.write(30, 2, 'Average Scheduled Headway (min)')      
        worksheet.write(31, 2, 'Average Full Fare ($)')      
        worksheet.write(32, 2, 'Average Distance Traveled per Passenger (mi)')      
        worksheet.write(33, 2, 'Average In-Vehicle Time per Passenger (min)')      
        worksheet.write(34, 2, 'Average Wait Time per Passenger (min)')      

        worksheet.set_row(27, None, dec_format) 
        worksheet.set_row(28, None, dec_format) 
        worksheet.set_row(29, None, dec_format) 
        worksheet.set_row(30, None, dec_format) 
        worksheet.set_row(31, None, money_format) 
        worksheet.set_row(32, None, dec_format) 
        worksheet.set_row(33, None, dec_format) 
        worksheet.set_row(34, None, dec_format) 
 
        selected = df[['RUNSPEED', 'TOTSPEED', 'DWELL_PER_STOP', 'HEADWAY_S', 'FARE_PER_PASS', 'MILES_PER_PASS', 'IVT_PER_PAS', 'WAIT_PER_PAS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=27, startcol=4, header=False, index=False)
        
        for r in range(27,35):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
                        
        # RELIABILITY
        worksheet.write(35, 1, 'Reliability', bold)    
        worksheet.write(36, 2, 'Percent of Vehicles Arriving On-Time (-1 to +5 min)')       
        worksheet.write(37, 2, 'Average Waiting Delay per Passenger (min)')       
        worksheet.write(38, 2, 'Average Arrival Delay per Passenger (min)')       

        worksheet.set_row(36, None, percent_format) 
        worksheet.set_row(37, None, dec_format) 
        worksheet.set_row(38, None, dec_format) 

        selected = df[['ONTIME5', 'DELAY_DEP_PER_PASS', 'DELAY_ARR_PER_PASS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=36, startcol=4, header=False, index=False)
        
        for r in range(36,39):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
            
        # CROWDING
        worksheet.write(39, 1, 'Crowding', bold)   
        worksheet.write(40, 2, 'Average Volume-Capacity Ratio')       
        worksheet.write(41, 2, 'Percent of Trips with V/C > 0.85')       
        
        selected = df[['VC', 'CROWDED', 'CROWDHOURS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=40, startcol=4, header=False, index=False)       
        
        worksheet.set_row(40, None, dec_format) 
        worksheet.set_row(41, None, percent_format) 

        for r in range(40,42):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})

        # OBSERVATIONS & ERROR
        worksheet.write(42, 1, 'Observations & Error', bold)       
        worksheet.write(43, 2, 'Number of Days')       
        worksheet.write(44, 2, 'Days with Observations')       
        worksheet.write(45, 2, 'Percent of Trips Observed')      
        worksheet.write(46, 2, 'Measurement Error (ON/OFF-1)')       
        worksheet.write(47, 2, 'Weighting Error (SERVMILES/SERVMILES_S-1)')      

        worksheet.set_row(43, None, int_format) 
        worksheet.set_row(44, None, int_format) 
        worksheet.set_row(45, None, percent_format) 
        worksheet.set_row(46, None, percent_format)
        worksheet.set_row(47, None, percent_format)

        selected = df[['NUMDAYS', 'OBSDAYS', 'OBSERVED_PCT', 'MEASURE_ERR', 'WEIGHT_ERR']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=43, startcol=4, header=False, index=False)  
        
        for r in range(43,48):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
            
            
    def writeSystemDifferenceFormulas(self, writer, months, tod): 
        '''
        Adds formulas to the system worksheet to calculate differences
        from 12 months earlier. 
        '''
        # which cells to look at
        ROW_OFFSET = 40
        COL_OFFSET = 12
        max_col = 3+len(months)+1
        
        # get the worksheet
        workbook  = writer.book
        worksheet = writer.sheets[tod]        
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        int_format = workbook.add_format({'num_format': '#,##0'})
        dec_format = workbook.add_format({'num_format': '#,##0.00'})
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # the header and labels
        worksheet.write(50,4, 'Difference from 12 Months Before', bold)
        worksheet.write(51,3, 'Difference Trend', bold)
        months.T.to_excel(writer, sheet_name=tod, 
                            startrow=51, startcol=4, header=False, index=False)   
        
        
        # DRIVERS OF DEMAND
        worksheet.write(52, 1, 'Drivers of Demand', bold)        
        
        for r in range(53,56):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, int_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                
        # SERVICE
        worksheet.write(56, 1, 'Service Provided', bold)        
        
        for r in range(57,59):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, int_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
        
        # RIDERSHIP
        worksheet.write(59, 1, 'Ridership', bold)      
            
        for r in range(60,66):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, int_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
        
        # LEVEL-OF-SERVICE
        worksheet.write(66, 1, 'Level-of-Service', bold)      
        
        for r in range(67,75):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, dec_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                           
        worksheet.set_row(71, None, money_format) 
        
        # RELIABILITY
        worksheet.write(75, 1, 'Reliability', bold)    
        
        for r in range(76,79):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, dec_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                        
        worksheet.set_row(76, None, percent_format)      
        
        # CROWDING
        worksheet.write(79, 1, 'Crowding', bold)   
        
        for r in range(80,82):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, int_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(ISNUMBER('+old+'),'+new+'-'+old+')')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                          
        worksheet.set_row(80, None, dec_format)              
        worksheet.set_row(81, None, percent_format) 
        
        
    def writeSystemPercentDifferenceFormulas(self, writer, months, tod): 
        '''
        Adds formulas to the system worksheet to calculate percent differences
        from 12 months earlier. 
        '''
        # which cells to look at
        ROW_OFFSET = 74
        COL_OFFSET = 12
        max_col = 3+len(months)+1
        
        # get the worksheet
        workbook  = writer.book
        worksheet = writer.sheets[tod]        
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # the header and labels
        worksheet.write(84,4, 'Percent Difference from 12 Months Before', bold)
        worksheet.write(85,3, 'Percent Difference Trend', bold)
        months.T.to_excel(writer, sheet_name=tod, 
                            startrow=85, startcol=4, header=False, index=False)   
        
        # DRIVERS OF DEMAND
        worksheet.write(86, 1, 'Drivers of Demand', bold)        
        
        for r in range(87,90):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})   

        # SERVICE
        worksheet.write(90, 1, 'Service Provided', bold)        
        
        for r in range(91,93):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
        
        # RIDERSHIP
        worksheet.write(93, 1, 'Ridership', bold)      
            
        for r in range(94,100):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
        
        # LEVEL-OF-SERVICE
        worksheet.write(100, 1, 'Level-of-Service', bold)      
        
        for r in range(101,109):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                           
        
        # RELIABILITY
        worksheet.write(109, 1, 'Reliability', bold)    
        
        for r in range(110,113):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                        
        
        # CROWDING
        worksheet.write(113, 1, 'Crowding', bold)   
        
        for r in range(114,116):
            cell = xl_rowcol_to_cell(r, 2)
            label = xl_rowcol_to_cell(r-ROW_OFFSET, 2)
            worksheet.write_formula(cell, '='+label)
            worksheet.set_row(r, None, percent_format) 
            
            for c in range(4+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '='+new+'/'+old+'-1')
            
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 3, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})      
                


    def createRoutePlot(self, outfile, months, dow, tod, route_short_name, dir):
        '''
        Creates a plot of route load/performance, and writes to the specified
        HTML file. 
        '''
        
        (month1, month2) = months
        
        # format dates for printing
        ctime1  = pd.Timestamp(month1).to_datetime().ctime()
        ctime2  = pd.Timestamp(month2).to_datetime().ctime()        
        
        cmonth1 = ctime1[4:7] + ' ' + ctime1[20:]
        cmonth2 = ctime2[4:7] + ' ' + ctime2[20:]       
        
        # format title
        if (dir==1): 
            dir_string = ', Inbound'
        else: 
            dir_string = ', Outbound'
            
        if (dow==1):
            dow_string = ', Average Weekday'
        elif(dow==2): 
            dow_string = ', Average Saturday'
        elif(dow==3):
            dow_string = ', Average Sunday/Holiday'
            
        if (tod=='Daily'): 
            tod_string = ', Daily'
        else: 
            tod_string = ', Time Period ' + tod
        
        title='Route Profile: Route ' \
              + str(route_short_name) \
              + dir_string \
              + dow_string \
              + tod_string
        
        # get the data
        ts_store = pd.HDFStore(self.ts_file) 
        if tod=='Daily': 
            before = ts_store.select('rs_day', where="MONTH=Timestamp(month1) & DOW=dow & ROUTE_SHORT_NAME=route_short_name & DIR=dir") 
            after  = ts_store.select('rs_day', where="MONTH=Timestamp(month2) & DOW=dow & ROUTE_SHORT_NAME=route_short_name & DIR=dir") 
        else:
            before = ts_store.select('rs_tod', where="MONTH=Timestamp(month1) & DOW=dow & TOD=tod & ROUTE_SHORT_NAME=route_short_name & DIR=dir") 
            after  = ts_store.select('rs_tod', where="MONTH=Timestamp(month2) & DOW=dow & TOD=tod & ROUTE_SHORT_NAME=route_short_name & DIR=dir")  
        ts_store.close()
        
        # re-calculate the load after averaging
        load = 0.0
        for i, row in before.iterrows():
            load -= row['OFF']
            load += row['ON']
            before.at[i, 'LOAD_DEP'] = load
            
        load = 0.0
        for i, row in after.iterrows():
            load -= row['OFF']
            load += row['ON']
            after.at[i, 'LOAD_DEP'] = load
            
                                        
        #create the plot
        bk.output_file(outfile)        
        stop_labels = before['STOPNAME'].tolist()     
        p = bk.figure(plot_width=1000, # in units of px
                      plot_height=650,  
                      title=title, 
                      title_text_font_size='12pt', 
                      x_range = stop_labels                 
                      )    
               
        
        # plot the boardings and alightings as bar charts
        # y is the bottom of the rectangle, so adjust height accordingly
        
        #before 
        p.rect(x=before['SEQ']+0.1, 
               y=0.5*before['ON'], 
               width=0.2, 
               height=before['ON'], 
               color='steelblue', 
               legend=cmonth1 + ' Boardings')
               
        p.rect(x=before['SEQ']-0.4, 
               y=-0.5*before['OFF'], 
               width=0.2, 
               height=before['OFF'], 
               color='steelblue', 
               alpha=0.4, 
               legend=cmonth1 + ' Alightings')
               
        #after
        p.rect(x=after['SEQ']+0.3, 
               y=0.5*after['ON'], 
               width=0.2, 
               height=after['ON'], 
               color='crimson', 
               legend=cmonth2 + ' Boardings')
               
        p.rect(x=after['SEQ']-0.2, 
               y=-0.5*after['OFF'], 
               width=0.2, 
               height=after['OFF'], 
               color='crimson', 
               alpha=0.4, 
               legend=cmonth2 + ' Alightings')
               
               
        
        # plot the load as a line
        
        # before
        p.line(before['SEQ'], 
               before['LOAD_DEP'], 
               line_width=2, 
               line_color='steelblue', 
               legend=cmonth1 + ' Load')
               
        # after
        p.line(after['SEQ'], 
               after['LOAD_DEP'], 
               line_width=2, 
               line_color='crimson', 
               legend=cmonth2 + ' Load')
               
               
        
        # do some formatting
        p.yaxis.axis_label = "Passengers"
        p.yaxis.axis_label_text_font_size='12pt'
        
        p.xaxis.axis_label = "Stop"
        p.xaxis.axis_label_text_font_size='12pt'
        
        p.xaxis.major_label_orientation = np.pi / 2.0
        p.xaxis.major_label_text_font_size = '8pt'
        
        p.legend.orientation = "top_left"
        
        
        # and write the output
        bk.show(p)
        


    def assembleDemandData(self, fips):
        '''
        Calculates the fields used in the  performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        demand_store = pd.HDFStore(self.demand_file)
        multimodal_store = pd.HDFStore(self.multimodal_file)
        
        if fips=='Total': 
            population = demand_store.select('totalPop')
            acs        = demand_store.select('totalACS')
            hu         = demand_store.select('countyHousingUnits', where="FIPS=fips")
            employment = demand_store.select('totalEmp')
            lodesWAC   = demand_store.select('lodesWACtotal')
            lodesRAC   = demand_store.select('lodesRACtotal')
            lodesOD    = demand_store.select('lodesODtotal')
            autoOpCost = demand_store.select('autoOpCost')
            tolls      = demand_store.select('tollCost')
            parkingCost= demand_store.select('parkingCost')
            transitFare= multimodal_store.select('transitFare')
        else: 
            population = demand_store.select('countyPop', where="FIPS=fips")
            acs        = demand_store.select('countyACS', where="FIPS=fips")
            hu         = demand_store.select('countyHousingUnits', where="FIPS=fips")
            employment = demand_store.select('countyEmp', where="FIPS=fips")
            lodesWAC   = demand_store.select('lodesWAC', where="FIPS=fips")
            lodesRAC   = demand_store.select('lodesRAC', where="FIPS=fips")
            lodesOD    = demand_store.select('lodesOD', where="FIPS=fips")
            autoOpCost = demand_store.select('autoOpCost')
            tolls      = demand_store.select('tollCost')
            parkingCost= demand_store.select('parkingCost')
            transitFare= multimodal_store.select('transitFare')
            
        demand_store.close()
        
        # start with the employment, which has the longest time-series, 
        # and join all the others with the month being equivalent
        df = employment
        df = pd.merge(df, population, how='left', on=['MONTH'], sort=True, suffixes=('', '_POP')) 
        df = pd.merge(df, acs, how='left', on=['MONTH'], sort=True, suffixes=('', '_ACS')) 
        df = pd.merge(df, hu, how='left', on=['MONTH'], sort=True, suffixes=('', '_HU')) 
        df = pd.merge(df, lodesWAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_WAC')) 
        df = pd.merge(df, lodesRAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_RAC')) 
        df = pd.merge(df, lodesOD, how='left', on=['MONTH'], sort=True, suffixes=('', '_OD')) 
        df = pd.merge(df, autoOpCost, how='left', on=['MONTH'], sort=True, suffixes=('', '_AOP')) 
        df = pd.merge(df, tolls, how='left', on=['MONTH'], sort=True, suffixes=('', '_TOLL')) 
        df = pd.merge(df, parkingCost, how='left', on=['MONTH'], sort=True, suffixes=('', '_PARK')) 
        df = pd.merge(df, transitFare, how='left', on=['MONTH'], sort=True, suffixes=('', '_FARE')) 
        
        # some additional, calculated fields        
        df['EMP_EARN0_40'] = df['EMP_EARN0_15'] + df['EMP_EARN15_40']
        df['WORKERS_EARN0_40'] = df['WORKERS_EARN0_15'] + df['WORKERS_EARN15_40']
        
        df['EmpPerHU'] = 1.0 * df['TOTEMP'] / df['UNITS']
        df['EmpPerWorker'] = 1.0 * df['TOTEMP'] / df['WORKERS_RAC']

        df['INTRA_SHARE_EMP'] = df['INTRA'] / df['TOTEMP']
        df['IN_SHARE_EMP'] = df['IN'] / df['TOTEMP']

        df['INTRA_SHARE_WKR'] = df['INTRA'] / df['WORKERS_RAC']
        df['OUT_SHARE_WKR'] = df['OUT'] / df['WORKERS_RAC']

        df['WKR_SHARE_POP'] = df['WORKERS_RAC'] / df['POP']
        
        df['NON_WORKERS'] = df['POP'] - df['WORKERS_RAC']
        
        df['EMP_SHARE0_15']  = df['EMP_EARN0_15']  / df['TOTEMP']
        df['EMP_SHARE15_40'] = df['EMP_EARN15_40'] / df['TOTEMP']
        df['EMP_SHARE40P']   = df['EMP_EARN40P']   / df['TOTEMP']
       
        df['WORKERS_SHARE0_15']  = df['WORKERS_EARN0_15']  / df['WORKERS_RAC']
        df['WORKERS_SHARE15_40'] = df['WORKERS_EARN15_40'] / df['WORKERS_RAC']
        df['WORKERS_SHARE40P']   = df['WORKERS_EARN40P']   / df['WORKERS_RAC']

        return df

        
    def writeDemandReport(self, xlsfile, fipsList, comments=None):
        '''
        Writes a drivers of demand for all months to the specified excel file.        
        '''    
        
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]
 
        # establish the writer        
        self.writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        
        
        fipsList.append(('Total', 'Total', 'Total'))
        
        # create a tab for each county
        for fips, countyName, abbreviation in fipsList: 
    
            # get the actual data
            df = self.assembleDemandData(fips)    
                        
            # Write the month as the column headers
            months = df[['MONTH']]
            months.T.to_excel(self.writer, sheet_name=countyName,  
                                    startrow=10, startcol=7, header=False, index=False)
                        
        
            # note that we have to call the pandas function first to get the
            # excel sheet created properly, so now we can access that
            worksheet = self.writer.sheets[countyName]
                
            # set up the formatting, with defaults
            bold = self.writer.book.add_format({'bold': 1})        
                
            # set the column widths
            worksheet.set_column(0, 0, 1)
            worksheet.set_column(1, 1, 3, bold)
            worksheet.set_column(2, 2, 45)
            worksheet.set_column(3, 3, 17)
            worksheet.set_column(4, 4, 15)
            worksheet.set_column(5, 5, 10)
            worksheet.set_column(6, 6, 25)                    
                
            # write the header
            worksheet.write(1, 1, 'Drivers of Demand Report', bold)
            worksheet.write(3, 1, 'Input Specification', bold)
            worksheet.write(4, 2, 'Geographic Extent: ')
            worksheet.write(4, 3, countyName)
            worksheet.write(5, 2, 'Temporal Resolution: ')
            worksheet.write(5, 3, 'Monthly')
            worksheet.write(6, 2, 'Report Generated on: ')
            worksheet.write(6, 3, timestring)
            worksheet.write(7, 2, 'Comments: ')      
            worksheet.write(8, 3, comments)        
                
            # Use formulas to calculate the differences
            self.writeDemandValues(df, months, countyName)
            self.writeDemandDifferenceFormulas(months, countyName)
            self.writeDemandPercentDifferenceFormulas(months, countyName)    
                
            # freeze so we can see what's happening
            worksheet.freeze_panes(0, 7)
            
        self.writer.save()
    

    def writeDemandValues(self, df, months, sheetName):
        '''
        Writes the main  values to the worksheet. 
        '''
    
        # get the worksheet
        worksheet = self.writer.sheets[sheetName]        
        
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})
        int_format = self.writer.book.add_format({'num_format': '#,##0'})
        dec_format = self.writer.book.add_format({'num_format': '#,##0.00'})
        cent_format = self.writer.book.add_format({'num_format': '$#,##0.00'})
        dollar_format = self.writer.book.add_format({'num_format': '$#,##0'})
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        
        # HEADER
        worksheet.write(9, 7, 'Values', bold)
        worksheet.write(10, 3, 'Source', bold)        
        worksheet.write(10, 4, 'Temporal Res', bold)        
        worksheet.write(10, 5, 'Geog Res', bold)        
        worksheet.write(10, 6, 'Trend', bold)        
        
        self.set_position(self.writer, worksheet, 11, 2)
        
        # POPULATION & HOUSEHOLDS
        worksheet.write(self.row, 1, 'Population & Households', bold)
        self.row += 1
        
        self.write_row(label='Population', data=df[['POP']], 
            source='Census PopEst', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households', data=df[['HH']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Housing Units', data=df[['UNITS_ACS']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Housing Units', data=df[['UNITS']], 
            source='Planning Dept/Census', tempRes='Date', geogRes='Block', format=int_format)

        self.write_row(label='Households, Income $0-15k', data=df[['HH_INC0_15']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households, Income $15-50k', data=df[['HH_INC15_50']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households, Income $50-100k', data=df[['HH_INC50_100']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households, Income $100k+', data=df[['HH_INC100P']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households, 0 Vehicles', data=df[['HH_0VEH']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)
        
        self.write_row(label='Median Household Income (2010$)', data=df[['MEDIAN_HHINC_2010USD']], 
            source='ACS', tempRes='Annual', geogRes='County', format=dollar_format)
            
            
        # POPULATION & HOUSEHOLDS
        worksheet.write(self.row, 1, 'Workers (at home location)', bold)
        self.row += 1
        
        self.write_row(label='Workers', data=df[['WORKERS_RAC']], 
            source='LODES RAC/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Workers, earning $0-15k', data=df[['WORKERS_EARN0_15']], 
            source='LODES RAC/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)

        self.write_row(label='Workers, earning $15-40k', data=df[['WORKERS_EARN15_40']], 
            source='LODES RAC/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Workers, earning $40k+', data=df[['WORKERS_EARN40P']], 
            source='LODES RAC/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
        
            
        # EMPLOYMENT
        worksheet.write(self.row, 1, 'Employment (at work location)', bold)
        self.row += 1
        
        self.write_row(label='Total Employment', data=df[['TOTEMP']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Retail Employment', data=df[['RETAIL_EMP']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Education and Health Employment', data=df[['EDHEALTH_EMP']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Leisure Employment', data=df[['LEISURE_EMP']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Other Employment', data=df[['OTHER_EMP']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Employees, earning $0-15k', data=df[['EMP_EARN0_15']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Employees, earning $15-40k', data=df[['EMP_EARN15_40']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Employees, earning $40k+', data=df[['EMP_EARN40P']], 
            source='LODES WAC/QCEW', tempRes='Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Average monthly earnings (2010$)', data=df[['AVG_MONTHLY_EARNINGS_2010USD']], 
            source='QCEW', tempRes='Monthly', geogRes='County', format=dollar_format)

        # JOBS-HOUSING BALANCE
        worksheet.write(self.row, 1, 'Jobs-Housing Balance', bold)
        self.row += 1
        
        self.write_row(label='Employees per Housing Unit', data=df[['EmpPerHU']], 
            source='QCEW/Planning Dept', tempRes='Monthly', geogRes='Block', format=dec_format)

        self.write_row(label='Employees per Worker', data=df[['EmpPerWorker']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=dec_format)

        self.write_row(label='Workers: Live & Work in SF', data=df[['INTRA']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)

        self.write_row(label='Workers: Live elswhere & work in SF', data=df[['IN']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Workers: Live in SF & work elsewhere', data=df[['OUT']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        # COSTS
        worksheet.write(self.row, 1, 'Costs', bold)
        self.row += 1
        
        self.write_row(label='Average Fuel Price (2010$)', data=df[['FUEL_PRICE_2010USD']], 
            source='EIA', tempRes='Monthly', geogRes='MSA', format=cent_format)
            
        self.write_row(label='Average Fleet Efficiency (mpg)', data=df[['FLEET_EFFICIENCY']], 
            source='BTS', tempRes='Annual', geogRes='US', format=dec_format)
            
        self.write_row(label='Average Fuel Cost (2010$ / mi)', data=df[['FUEL_COST_2010USD']], 
            source='BTS/EIA', tempRes='Annual/Monthly', geogRes='US/MSA', format=cent_format)

        self.write_row(label='Average Auto Operating Cost (2010$/mile)', data=df[['IRS_MILEAGE_RATE_2010USD']], 
            source='IRS', tempRes='Annual', geogRes='US', format=cent_format)
            
        self.write_row(label='Median Daily CBD Parking Cost (2010$)', data=df[['DAILY_PARKING_RATE_2010USD']], 
            source='Colliers', tempRes='Annual', geogRes='CBD', format=cent_format)
            
        self.write_row(label='Median Monthly CBD Parking Cost (2010$)', data=df[['MONTHLY_PARKING_RATE_2010USD']], 
            source='Colliers', tempRes='Annual', geogRes='CBD', format=cent_format)

        self.write_row(label='Bay Bridge Toll, Peak (2010$)', data=df[['TOLL_BB_PK_2010USD']], 
            source='BATA', tempRes='Monthly', geogRes='Bridge', format=cent_format)
            
        self.write_row(label='Bay Bridge Toll, Off-Peak (2010$)', data=df[['TOLL_BB_OP_2010USD']], 
            source='BATA', tempRes='Monthly', geogRes='Bridge', format=cent_format)

        self.write_row(label='Bay Bridge Toll, Carpools (2010$)', data=df[['TOLL_BB_CARPOOL_2010USD']], 
            source='BATA', tempRes='Monthly', geogRes='Bridge', format=cent_format)

        self.write_row(label='Golden Gate Bridge Toll, Peak (2010$)', data=df[['TOLL_GGB_2010USD']], 
            source='BATA', tempRes='Monthly', geogRes='Bridge', format=cent_format)

        self.write_row(label='Golden Gate Bridge Toll, Carpools (2010$)', data=df[['TOLL_GGB_CARPOOL_2010USD']], 
            source='BATA', tempRes='Monthly', geogRes='Bridge', format=cent_format)

        self.write_row(label='Consumer Price Index', data=df[['CPI']], 
            source='BLS', tempRes='Monthly', geogRes='US City Avg', format=int_format)
            


    def writeDemandDifferenceFormulas(self, months, sheetName): 
        '''
        Adds formulas to the  worksheet to calculate differences
        from 12 months earlier. 
        '''
        # which cells to look at
        ROW_OFFSET = 49
        COL_OFFSET = 12
        max_col = 6+len(months)+1
        
        # get the worksheet
        workbook  = self.writer.book
        worksheet = self.writer.sheets[sheetName]        
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        int_format = workbook.add_format({'num_format': '#,##0'})
        dec_format = workbook.add_format({'num_format': '#,##0.00'})
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # the header and labels
        worksheet.write(58, 7, 'Difference from 12 Months Before', bold)
        worksheet.write(59, 3, 'Source', bold)        
        worksheet.write(59, 4, 'Temporal Res', bold)        
        worksheet.write(59, 5, 'Geog Res', bold)    
        worksheet.write(59, 6, 'Difference Trend', bold)
        months.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=59, startcol=7, header=False, index=False)   
        
        # the data: 
        for r in range(60,106):
            for c in range(2, 6): 
                cell = xl_rowcol_to_cell(r, c)
                label = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                worksheet.write_formula(cell, '=IF(ISTEXT('+label+'),'+label+',"")')
            
            for c in range(7+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'-'+old+',"")')
            
            data_range = xl_rowcol_to_cell(r, 7) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 6, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})                  
               
               
        # set the headers and formats                    
        worksheet.write(60,  1, 'Population & Households', bold)
        worksheet.write_blank(60,  2, None, bold)
        for r in range(61,71):                     
            worksheet.set_row(r, None, int_format) 

        worksheet.write(71,  1, 'Workers (at home location)', bold)
        worksheet.write_blank(71,  2, None, bold)
        for r in range(72,76):                     
            worksheet.set_row(r, None, int_format) 

        worksheet.write(76, 1, 'Employment (at work location)', bold)
        worksheet.write_blank(76,  2, None, bold)
        for r in range(77,86):                     
            worksheet.set_row(r, None, int_format) 

        worksheet.write(86, 1, 'Jobs-Housing Balance', bold)
        worksheet.write_blank(86,  2, None, bold)
        for r in range(87,89):                     
            worksheet.set_row(r, None, dec_format) 
        for r in range(89,92):                     
            worksheet.set_row(r, None, int_format) 

        worksheet.write(92, 1, 'Costs', bold)
        worksheet.write_blank(92,  2, None, bold)
        for r in range(93,106):                     
            worksheet.set_row(r, None, money_format) 



    def writeDemandPercentDifferenceFormulas(self, months, sheetName): 
        '''
        Adds formulas to the  worksheet to calculate percent differences
        from 12 months earlier. 
        '''
        # which cells to look at
        ROW_OFFSET = 98
        COL_OFFSET = 12
        max_col = 6+len(months)+1
        
        # get the worksheet
        workbook  = self.writer.book
        worksheet = self.writer.sheets[sheetName]        
        
        # set up the formatting, with defaults
        bold = workbook.add_format({'bold': 1})
        int_format = workbook.add_format({'num_format': '#,##0'})
        dec_format = workbook.add_format({'num_format': '#,##0.00'})
        money_format = workbook.add_format({'num_format': '$#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.0%'})
        
        # the header and labels
        worksheet.write(107, 7, 'Percent Difference from 12 Months Before', bold)
        worksheet.write(108, 3, 'Source', bold)        
        worksheet.write(108, 4, 'Temporal Res', bold)        
        worksheet.write(108, 5, 'Geog Res', bold)    
        worksheet.write(108, 6, 'Percent Difference Trend', bold)
        months.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=108, startcol=7, header=False, index=False)   
        
        # the data
        for r in range(109,155):
            for c in range(2, 6): 
                cell = xl_rowcol_to_cell(r, c)
                label = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                worksheet.write_formula(cell, '=IF(ISTEXT('+label+'),'+label+',"")')
            
            worksheet.set_row(r, None, percent_format) 

            for c in range(7+COL_OFFSET, max_col):
                cell = xl_rowcol_to_cell(r, c)
                new = xl_rowcol_to_cell(r-ROW_OFFSET, c)
                old = xl_rowcol_to_cell(r-ROW_OFFSET, c-COL_OFFSET)
                worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'/'+old+'-1,"")')
            
            data_range = xl_rowcol_to_cell(r, 7) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(r, 6, {'range': data_range, 
                                           'type': 'column', 
                                           'negative_points': True})                  
               
        # set the headers and formats                    
        worksheet.write(109,  1, 'Population & Households', bold)
        worksheet.write_blank(109,  2, None, bold)

        worksheet.write(120,  1, 'Workers (at home location)', bold)
        worksheet.write_blank(120,  2, None, bold)

        worksheet.write(125, 1, 'Employment (at work location)', bold)
        worksheet.write_blank(125,  2, None, bold)

        worksheet.write(135, 1, 'Jobs-Housing Balance', bold)
        worksheet.write_blank(135,  2, None, bold)

        worksheet.write(141, 1, 'Costs', bold)
        worksheet.write_blank(141,  2, None, bold)

        


    def assembleAnnualMultiModalData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        mm_store = pd.HDFStore(self.multimodal_file)
        demand_store = pd.HDFStore(self.demand_file)
        
        transit = mm_store.select('transitAnnual')
        fares = mm_store.select('transitFareAnnual')
        acs = demand_store.select('totalACSannual')
        
        mm_store.close()
        demand_store.close()

        # start with the population, which has the longest time-series, 
        # and join all the others with the month being equivalent
        df = pd.merge(transit, fares, how='left', on=['FISCAL_YEAR'],  sort=True, suffixes=('', '_FARE')) 
        df = pd.merge(df, acs, how='left', left_on=['FISCAL_YEAR'], right_on=['YEAR'], sort=True, suffixes=('', '_ACS')) 

        return df


    def assembleMonthlyMultiModalData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        mm_store = pd.HDFStore(self.multimodal_file)
        demand_store = pd.HDFStore(self.demand_file)
        ts_store = pd.HDFStore(self.ts_file)
        gtfs_store = pd.HDFStore(self.gtfs_file)
        
        transit = mm_store.select('transitMonthly')
        fares = mm_store.select('transitFare')
        acs = demand_store.select('totalACS')
        bart = mm_store.select('bart_weekday', where="FROM='Entries' and TO='Exits'")
        
        # schedule data
        gtfs_bart     = gtfs_store.select('bartMonthly', where='DOW=1 and ROUTE_TYPE=1')
        gtfs_munibus  = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=3')
        gtfs_munirail = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=0')
        gtfs_municc   = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=5')     
        
        # extrapolated schedule data
        servmiles_extrapolated = mm_store.select('exrapolatedServiceMiles')
        
        # more specific data
        ts = ts_store.select('system_day_s', where='DOW=1') 
        muni = ts[['MONTH']].copy()
        muni['APC_ON_MUNI_BUS'] = ts['ON']
        bart['APC_ON_BART'] = bart['RIDERS']
        
        mm_store.close()
        demand_store.close()
        ts_store.close()
        gtfs_store.close()

        # join all the others with the month being equivalent
        df = pd.merge(bart, transit, how='left', on=['MONTH'],  sort=True, suffixes=('', '_PERFREPORT')) 
        df = pd.merge(df, fares, how='left', on=['MONTH'],  sort=True, suffixes=('', '_FARE')) 
        df = pd.merge(df, acs, how='left', on=['MONTH'],  sort=True, suffixes=('', '_ACS')) 
        df = pd.merge(df, muni, how='left', on=['MONTH'],  sort=True, suffixes=('', '_MUNI')) 

        # do the first one twice so we get the suffixes right
        df = pd.merge(df, gtfs_bart, how='left', on=['MONTH'],  sort=True, suffixes=('', '_NOT_USED')) 
        df = pd.merge(df, gtfs_bart, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_BART')) 
        df = pd.merge(df, gtfs_munibus, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_BUS')) 
        df = pd.merge(df, gtfs_munirail, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_RAIL')) 
        df = pd.merge(df, gtfs_municc, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_CC')) 
        
        # merge extrapolated service miles
        df = pd.merge(df, servmiles_extrapolated, how='left', on=['MONTH'], sort=True, suffixes=('', '_EXTRAP'))
        
        return df

        
    def writeMultiModalReport(self, xlsfile, comments=None):
        '''
        Writes a drivers of demand for all months to the specified excel file.        
        '''    
         
        # establish the writer        
        self.writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        
        
        self.writeAnnualMultiModalSheet(sheetName='Fiscal Year', comments=comments)
        self.writeMonthlyMultiModalSheet(sheetName='Monthly', comments=comments)
        
        self.writer.save()

    
    def writeAnnualMultiModalSheet(self, sheetName, comments=None):         
                    
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]

        # get the actual data
        df = self.assembleAnnualMultiModalData()    

        # Write the month as the column headers
        years = df[['FISCAL_YEAR']]
        years.T.to_excel(self.writer, sheet_name=sheetName,  
                                startrow=10, startcol=7, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})        
            
        # set the column widths
        worksheet.set_column(0, 0, 1)
        worksheet.set_column(1, 1, 3, bold)
        worksheet.set_column(2, 2, 35)
        worksheet.set_column(3, 3, 20)
        worksheet.set_column(4, 4, 15)
        worksheet.set_column(5, 5, 10)
        worksheet.set_column(6, 6, 25)    
        worksheet.set_column(7, 1000, 12)                
            
        # write the header
        worksheet.write(1, 1, 'San Francisco MultiModal Performance Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, 'San Francisco County')
        worksheet.write(5, 2, 'Temporal Resolution: ')
        worksheet.write(5, 3, 'Fiscal Year')
        worksheet.write(6, 2, 'Report Generated on: ')
        worksheet.write(6, 3, timestring)
        worksheet.write(7, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
            
        # Use formulas to calculate the differences
        self.set_position(self.writer, worksheet, 7, 2)  
        self.writeAnnualMultiModalValues(df, years, sheetName, 'values')
        self.writeAnnualMultiModalValues(df, years, sheetName, 'diff')
        self.writeAnnualMultiModalValues(df, years, sheetName, 'pctDiff')
            
        # freeze so we can see what's happening
        worksheet.freeze_panes(0, 7)
            

    def writeAnnualMultiModalValues(self, df, years, sheetName, formulaType='values'):
        '''
        Writes the main system values to the worksheet. 
        '''
                
        # leave a couple of empty spaces
        self.row += 2
        
        # which cells to look at
        ROW_OFFSET = self.row - 9
        COL_OFFSET = 1
        max_col = 6+len(years)+1

        # get the worksheet
        worksheet = self.writer.sheets[sheetName]      
        
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})
        int_format = self.writer.book.add_format({'num_format': '#,##0'})
        dec_format = self.writer.book.add_format({'num_format': '#,##0.00'})
        cent_format = self.writer.book.add_format({'num_format': '$#,##0.00'})
        dollar_format = self.writer.book.add_format({'num_format': '$#,##0'})
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        
        # HEADER
        if formulaType=='diff': 
            worksheet.write(self.row, 7, 'Difference from year before', bold)
        elif formulaType=='pctDiff': 
            worksheet.write(self.row, 7, 'Percent difference from year before', bold)
        else:        
            worksheet.write(self.row, 7, 'Values', bold)
        self.row += 1
        
        worksheet.write(self.row, 3, 'Source', bold)        
        worksheet.write(self.row, 4, 'Temporal Res', bold)        
        worksheet.write(self.row, 5, 'Geog Res', bold)        
        worksheet.write(self.row, 6, 'Trend', bold) 
        years.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=self.row, startcol=7, header=False, index=False) 
        self.row += 1       

        
        # TRANSIT STATISTICAL SUMMARY DATA
        measures = [('Annual Service Miles', 'SERVMILES', 'Transit Stat Summary', 'FY', int_format), 
                    ('Annual Ridership', 'PASSENGERS', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Weekday Ridership', 'AVG_WEEKDAY_RIDERSHIP', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Fare (2010$)', 'AVG_FARE_2010USD', 'Transit Stat Summary', 'FY', cent_format), 
                    ('Cash Fare (2010$)', 'CASH_FARE_2010USD', 'Published Values', 'Actual', cent_format), 
                    ]
        
        modes = [('Muni Bus+Rail', 'MUNI'), 
                 ('Muni Bus', 'MUNI_BUS'),
                 ('Muni Cable Car', 'MUNI_CC'),
                 ('Muni Rail', 'MUNI_RAIL'),
                 ('BART', 'BART'),
                 ('Caltrain', 'CALTRAIN')
                ]
                
        for header, measure, source, tempRes, format in measures: 
            worksheet.write(self.row, 1, header, bold)
            self.row += 1
                
            for label, mode in modes: 
                if measure + '_' + mode in df.columns: 

                    if formulaType=='values':
                        self.write_row(label=label, data=df[[measure + '_' + mode]], 
                            source=source, tempRes=tempRes, geogRes='System', format=format)

                    else: 
                        self.write_difference_row(label=label, 
                            row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                            source=source, tempRes=tempRes, geogRes='System', format=format, 
                            formulaType=formulaType)

        # MODE SHARES        
        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK',    'Walk'),
                 ('OTHER',   'Taxi, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares', bold)
        self.row += 1

        for mode, modeName in modes: 
            key = 'JTW_' + mode + '_SHARE'
            label =  modeName
            
            if formulaType=='values':
                self.write_row(label=label, data=df[[key]], 
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
            else: 
                self.write_difference_row(label=label, 
                    row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                            formulaType=formulaType)


        # MODE SHARES BY SEGMENT   
        groups = [('JTW_EARN0_50_',    'Workers earning $0-50k: '),
                  ('JTW_EARN50P_',     'Workers earning $50k+: '),
                  ('JTW_0VEH_',        'Workers with 0 vehicles: '),
                  ('JTW_1PVEH_',       'Workers with 1+ vehicles: ')]

        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK_OTHER','Taxi, walk, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares by Segment', bold)
        self.row += 1

        for group, groupName in groups: 
            for mode, modeName in modes: 

                key = group + mode + '_SHARE'
                label = groupName + modeName

                if formulaType=='values':         
                    self.write_row(label=label, data=df[[key]], 
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
                else: 
                    self.write_difference_row(label=label, 
                        row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                                formulaType=formulaType)


    def writeMonthlyMultiModalSheet(self, sheetName, comments=None):         
                    
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]

        # get the actual data
        df = self.assembleMonthlyMultiModalData()    

        # Write the month as the column headers
        periods = df[['MONTH']]
        periods.T.to_excel(self.writer, sheet_name=sheetName,  
                                startrow=10, startcol=7, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})        
            
        # set the column widths
        worksheet.set_column(0, 0, 1)
        worksheet.set_column(1, 1, 3, bold)
        worksheet.set_column(2, 2, 35)
        worksheet.set_column(3, 3, 20)
        worksheet.set_column(4, 4, 15)
        worksheet.set_column(5, 5, 10)
        worksheet.set_column(6, 6, 25)    
        worksheet.set_column(7, 1000, 12)                
            
        # write the header
        worksheet.write(1, 1, 'San Francisco MultiModal Performance Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, 'San Francisco County')
        worksheet.write(5, 2, 'Temporal Resolution: ')
        worksheet.write(5, 3, 'Month')
        worksheet.write(6, 2, 'Report Generated on: ')
        worksheet.write(6, 3, timestring)
        worksheet.write(7, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
            
        # Use formulas to calculate the differences
        self.set_position(self.writer, worksheet, 7, 2)  
        self.writeMonthlyMultiModalValues(df, periods, sheetName, 'values')
        self.writeMonthlyMultiModalValues(df, periods, sheetName, 'diff')
        self.writeMonthlyMultiModalValues(df, periods, sheetName, 'pctDiff')
                        
        # freeze so we can see what's happening
        worksheet.freeze_panes(0, 7)


    def writeMonthlyMultiModalValues(self, df, months, sheetName, formulaType='values'):
        '''
        Writes the main system values to the worksheet. 
        '''
                
        # leave a couple of empty spaces
        self.row += 2
        
        # which cells to look at
        ROW_OFFSET = self.row - 9
        COL_OFFSET = 12
        max_col = 6+len(months)+1

        # get the worksheet
        worksheet = self.writer.sheets[sheetName]        
        
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})
        int_format = self.writer.book.add_format({'num_format': '#,##0'})
        dec_format = self.writer.book.add_format({'num_format': '#,##0.00'})
        cent_format = self.writer.book.add_format({'num_format': '$#,##0.00'})
        dollar_format = self.writer.book.add_format({'num_format': '$#,##0'})
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        
        
        # HEADER
        if formulaType=='diff': 
            worksheet.write(self.row, 7, 'Difference from year before', bold)
        elif formulaType=='pctDiff': 
            worksheet.write(self.row, 7, 'Percent difference from year before', bold)
        else:        
            worksheet.write(self.row, 7, 'Values', bold)
        self.row += 1
        
        worksheet.write(self.row, 3, 'Source', bold)        
        worksheet.write(self.row, 4, 'Temporal Res', bold)        
        worksheet.write(self.row, 5, 'Geog Res', bold)        
        worksheet.write(self.row, 6, 'Trend', bold) 
        months.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=self.row, startcol=7, header=False, index=False) 
        self.row += 1       
        
        # TRANSIT STATISTICAL SUMMARY DATA
        measures = [('Monthly Service Miles', 'SERVMILES', 'Transit Stat Summary', 'FY', int_format),
                    ('Average Weekday Ridership', 'AVG_WEEKDAY_RIDERSHIP', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Weekday Ridership', 'APC_ON', 'APCs/Faregate', 'Monthly', int_format), 
                    ('Cash Fare (2010$)', 'CASH_FARE_2010USD', 'Published Values', 'Actual', cent_format),  
                    ('Average Fare (2010$)', 'AVG_FARE_2010USD', 'Transit Stat Summary', 'FY/Actual', cent_format),
                    ('Weekday Stops', 'STOPS_GTFS', 'GTFS', 'Actual', int_format),  
                    ('Weekday Service Miles', 'SERVMILES_S_GTFS', 'GTFS', 'Actual', int_format), 
                    ('Weekday Service Miles-Extrapolated', 'SERVMILES_E', 'Stat Summary/GTFS', 'Monthly', int_format),   
                    ('Weekday Average Headway', 'HEADWAY_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ('Weekday Average Run Speed', 'RUNSPEED_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ('Weekday Average Total Speed', 'TOTSPEED_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ]
        
        
        modes = [('Muni Bus+Rail', 'MUNI'), 
                 ('Muni Bus', 'MUNI_BUS'),
                 ('Muni Cable Car', 'MUNI_CC'),
                 ('Muni Rail', 'MUNI_RAIL'),
                 ('BART', 'BART'),
                 ('Caltrain', 'CALTRAIN')
                ]
        
        for c in df.columns: 
            print c               
                                                                                            
        for header, measure, source, tempRes, format in measures: 
            worksheet.write(self.row, 1, header, bold)
            self.row += 1
                
            for label, mode in modes: 
                if measure + '_' + mode in df.columns:                     
                    if formulaType=='values':
                        self.write_row(label=label, data=df[[measure + '_' + mode]], 
                            source=source, tempRes=tempRes, geogRes='System', format=format)

                    else: 
                        self.write_difference_row(label=label, 
                            row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                            source=source, tempRes=tempRes, geogRes='System', format=format, 
                            formulaType=formulaType)
                else: 
                    print measure + '_' + mode + ' not found'


        # MODE SHARES        
        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK',    'Walk'),
                 ('OTHER',   'Taxi, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares', bold)
        self.row += 1

        for mode, modeName in modes: 
            key = 'JTW_' + mode + '_SHARE'
            label =  modeName
            
            if formulaType=='values':
                self.write_row(label=label, data=df[[key]], 
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
            else: 
                self.write_difference_row(label=label, 
                    row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                            formulaType=formulaType)


        # MODE SHARES BY SEGMENT   
        groups = [('JTW_EARN0_50_',    'Workers earning $0-50k: '),
                  ('JTW_EARN50P_',     'Workers earning $50k+: '),
                  ('JTW_0VEH_',        'Workers with 0 vehicles: '),
                  ('JTW_1PVEH_',       'Workers with 1+ vehicles: ')]

        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK_OTHER','Taxi, walk, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares by Segment', bold)
        self.row += 1

        for group, groupName in groups: 
            for mode, modeName in modes: 

                key = group + mode + '_SHARE'
                label = groupName + modeName

                if formulaType=='values':
                    self.write_row(label=label, data=df[[key]], 
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
                else: 
                    self.write_difference_row(label=label, 
                        row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                                formulaType=formulaType)


                        
        
    def writeSFMuniEstimationFile(self, estfile, fips, dow=1, tod='Daily'):
        '''
        Writes a model estimation file for SF MUNI busses.        
        '''     
                
        # get a filename for the differences
        base = os.path.splitext(estfile)
        estfileDiff = base[0] + '_diff' + base[1]


        # get the data
        muni = self.assembleSystemPerformanceData(fips, dow=dow, tod=tod)
        multimodal = self.assembleMonthlyMultiModalData()
        demand = self.assembleDemandData(fips)
                
        # interpolate for one missing month
        muni = muni.interpolate()
        
        # merge the data        
        df = muni
        df = pd.merge(df, multimodal, how='left', on=['MONTH'], sort=True, suffixes=('', '_MM')) 
        df = pd.merge(df, demand, how='left', on=['MONTH'], sort=True, suffixes=('', '_DEMAND')) 
        
        # calcluate the diff from 12 months before
        diff = pd.DataFrame()
        for col in df.columns: 
            if df[col].dtype=='object': 
                diff[col] = df[col] + '-' + df[col].shift(12)
            elif df[col].dtype=='datetime64[ns]':
                diff[col] = df[col]
            else: 
                diff[col] = df[col] - df[col].shift(12)
        diff = diff[12:]
        
        # and a bit of clean up
        diff['MONTH_NUM'] = diff['MONTH'].apply(lambda x: x.month)

        # write the data
        df.to_csv(estfile)
        diff.to_csv(estfileDiff)


    def writeBARTEstimationFile(self, estfile, fipsList):
        '''
        Writes a model estimation file for BART ridership       
        '''     
        
        # get a filename for the differences
        base = os.path.splitext(estfile)
        estfileDiff = base[0] + '_diff' + base[1]


        # get the basic data, including the 4-county total demand data
        multimodal = self.assembleMonthlyMultiModalData()
        demand = self.assembleDemandData('Total')
                
        # merge the data      
        df = pd.merge(multimodal, demand, how='left', on=['MONTH'], sort=True, suffixes=('', '_DEMAND')) 
        
        # now, merge the county-specific demand data
        for fips, countyName, abbreviation in fipsList: 
            demand = self.assembleDemandData(fips)
            df = pd.merge(df, demand, how='left', on=['MONTH'], sort=True, suffixes=('', '_' + abbreviation)) 
        
        # additional fields
        for col in demand.columns: 
            if col+'_Total' in df.columns: 
                if (df[col+'_Total'].dtype==np.float64) or (df[col+'_Total'].dtype==np.int64) : 
                    df[col+'_3COUNTY'] = df[col+'_Total'] - df[col+'_SFC']
                    df[col+'_SFSHARE'] = df[col+'_SFC'] / df[col+'_Total'] 
        
        # calcluate the diff from 12 months before
        diff = pd.DataFrame()
        for col in df.columns: 
            if df[col].dtype=='object': 
                diff[col] = df[col] + '-' + df[col].shift(12)
            elif df[col].dtype=='datetime64[ns]':
                diff[col] = df[col]
            else: 
                diff[col] = df[col] - df[col].shift(12)
        diff = diff[12:]
        
        # and a bit of clean up
        diff['MONTH_NUM'] = diff['MONTH'].apply(lambda x: x.month)

        # write the data
        df.to_csv(estfile)
        diff.to_csv(estfileDiff)



    def set_position(self, writer, worksheet, row, col):
        '''
        sets the position where the next item will be written. 
        
        '''

        self.writer = writer
        self.worksheet = worksheet
        self.row = row
        self.col = col
        
        
                                    
    def write_row(self, label, source, tempRes, geogRes, data, format, sparkline=True):
        '''
        Writes a row of data to the worksheet.
        
        worksheet - where to write it
        r - start row
        c - start column
        label - string label to write in first column
        source - string source to write in second column
        res - string resolution to write in third column
        format - number format for the row
        sparkline - boolean indicating whether or not to add a sparkline
        
        '''
        
        # labels
        self.worksheet.write(self.row, self.col, label)
        self.worksheet.write(self.row, self.col+1, source)
        self.worksheet.write(self.row, self.col+2, tempRes)
        self.worksheet.write(self.row, self.col+3, geogRes)

        # data
        self.worksheet.set_row(self.row, None, format) 
        data.T.to_excel(self.writer, sheet_name=self.worksheet.get_name(), 
                            startrow=self.row, startcol=self.col+5, header=False, index=False)
        
        # sparkline
        if sparkline: 
            cell = xl_rowcol_to_cell(self.row, self.col+4)
            data_range = (xl_rowcol_to_cell(self.row, self.col+5) + 
                   ':' + xl_rowcol_to_cell(self.row, self.col+5+len(data)+1))
            self.worksheet.add_sparkline(cell, {'range': data_range})   

        # increment the row
        self.row += 1

                                    
    def write_difference_row(self, row_offset, col_offset, max_col, 
        label, source, tempRes, geogRes, format, sparkline=True, 
        formulaType='diff'):
        '''
        Writes a row of difference formulas to the worksheet.
        
        worksheet - where to write it
        row_offset - offset for row formulas
        col_offset - offset for col formulas
        max_col - right most column
        label - string label to write in first column
        source - string source to write in second column
        res - string resolution to write in third column
        format - number format for the row
        sparkline - boolean indicating whether or not to add a sparkline
        formulaType - either 'diff' or 'pctDiff'
        '''
        
        # labels
        self.worksheet.write(self.row, self.col, label)
        self.worksheet.write(self.row, self.col+1, source)
        self.worksheet.write(self.row, self.col+2, tempRes)
        self.worksheet.write(self.row, self.col+3, geogRes)

        # formats
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        if formulaType=='diff': 
            self.worksheet.set_row(self.row, None, format) 
        elif formulaType=='pctDiff': 
            self.worksheet.set_row(self.row, None, percent_format)             
        
        # formulas
        for c in range(7+col_offset, max_col):
            cell = xl_rowcol_to_cell(self.row, c)
            new = xl_rowcol_to_cell(self.row-row_offset, c)
            old = xl_rowcol_to_cell(self.row-row_offset, c-col_offset)
            if formulaType=='diff': 
                self.worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'-'+old+',"")')
            elif formulaType=='pctDiff':                 
                self.worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'/'+old+'-1,"")')
            
        # sparkline
        if sparkline: 
            data_range = xl_rowcol_to_cell(self.row, 7) + ':' + xl_rowcol_to_cell(self.row, max_col)
            self.worksheet.add_sparkline(self.row, 6, {'range': data_range, 
                                                       'type': 'column', 
                                                       'negative_points': True})                  
        # increment the row
        self.row += 1
        
