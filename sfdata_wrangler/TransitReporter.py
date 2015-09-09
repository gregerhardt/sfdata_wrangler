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

    def __init__(self, trip_file, ts_file, demand_file):
        '''
        Constructor. 

        '''   
        self.trip_file = trip_file
        self.ts_file = ts_file
        self.demand_file = demand_file


    def assembleSystemPerformanceData(self, dow=1, tod='Daily'):
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

        employment = demand_store.select('countyEmp')
        population = demand_store.select('countyPop')
        fuelPrice  = demand_store.select('fuelPrice')

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
        
        # merge the drivers of demand data
        # employment includes TOTEMP
        df = pd.merge(df, employment, how='left', on=['MONTH'], sort=True) 
        
        # employment includes POP
        df = pd.merge(df, population, how='left', on=['MONTH'], sort=True)  
        
        # fuelPrice includes FUEL_PRICE and FUEL_PRICE_2010USD
        df = pd.merge(df, fuelPrice, how='left', on=['MONTH'], sort=True)  
        
        return df

        
    def writeSystemReport(self, xlsfile, 
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
            df = self.assembleSystemPerformanceData(dow=dow, tod=tod)    
                    
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
        worksheet.write(28, 2, 'Average Dwell Time per Stop (min)')      
        worksheet.write(29, 2, 'Average Scheduled Headway (min)')      
        worksheet.write(30, 2, 'Average Full Fare ($)')      
        worksheet.write(31, 2, 'Average Distance Traveled per Passenger (mi)')      
        worksheet.write(32, 2, 'Average In-Vehicle Time per Passenger (min)')      
        worksheet.write(33, 2, 'Average Wait Time per Passenger (min)')      

        worksheet.set_row(27, None, dec_format) 
        worksheet.set_row(28, None, dec_format) 
        worksheet.set_row(29, None, dec_format) 
        worksheet.set_row(30, None, money_format) 
        worksheet.set_row(31, None, dec_format) 
        worksheet.set_row(32, None, dec_format) 
        worksheet.set_row(33, None, dec_format) 
 
        selected = df[['RUNSPEED', 'DWELL_PER_STOP', 'HEADWAY_S', 'FARE_PER_PASS', 'MILES_PER_PASS', 'IVT_PER_PAS', 'WAIT_PER_PAS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=27, startcol=4, header=False, index=False)
        
        for r in range(27,34):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
                        
        # RELIABILITY
        worksheet.write(34, 1, 'Reliability', bold)    
        worksheet.write(35, 2, 'Percent of Vehicles Arriving On-Time (-1 to +5 min)')       
        worksheet.write(36, 2, 'Average Waiting Delay per Passenger (min)')       
        worksheet.write(37, 2, 'Average Arrival Delay per Passenger (min)')       

        worksheet.set_row(35, None, percent_format) 
        worksheet.set_row(36, None, dec_format) 
        worksheet.set_row(37, None, dec_format) 

        selected = df[['ONTIME5', 'DELAY_DEP_PER_PASS', 'DELAY_ARR_PER_PASS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=35, startcol=4, header=False, index=False)
        
        for r in range(34,38):
            cell = xl_rowcol_to_cell(r, 3)
            data_range = xl_rowcol_to_cell(r, 4) + ':' + xl_rowcol_to_cell(r, max_col)
            worksheet.add_sparkline(cell, {'range': data_range})
            
        # CROWDING
        worksheet.write(38, 1, 'Crowding', bold)   
        worksheet.write(39, 2, 'Average Volume-Capacity Ratio')       
        worksheet.write(40, 2, 'Percent of Trips with V/C > 0.85')       
        worksheet.write(41, 2, 'Passenger Hours with V/C > 0.85')        
        
        selected = df[['VC', 'CROWDED', 'CROWDHOURS']]
        selected.T.to_excel(writer, sheet_name=tod, 
                            startrow=39, startcol=4, header=False, index=False)       
        
        worksheet.set_row(39, None, dec_format) 
        worksheet.set_row(40, None, percent_format) 
        worksheet.set_row(41, None, int_format) 

        for r in range(39,42):
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
        
        for r in range(67,74):
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
                           
        worksheet.set_row(70, None, money_format) 
        
        # RELIABILITY
        worksheet.write(74, 1, 'Reliability', bold)    
        
        for r in range(75,78):
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
                        
        worksheet.set_row(75, None, percent_format)      
        
        # CROWDING
        worksheet.write(78, 1, 'Crowding', bold)   
        
        for r in range(79,82):
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
                          
        worksheet.set_row(79, None, dec_format)              
        worksheet.set_row(80, None, percent_format) 
        
        
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
        
        for r in range(101,108):
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
        worksheet.write(108, 1, 'Reliability', bold)    
        
        for r in range(109,112):
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
        worksheet.write(112, 1, 'Crowding', bold)   
        
        for r in range(113,116):
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
        
        