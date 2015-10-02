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

    
class DemandReporter():
    """ 
    Class to create drivers of demand reports and associated
    visuals and graphs. 
    """

    def __init__(self, demand_file):
        '''
        Constructor. 

        '''   
        self.demand_file = demand_file

        self.writer = None
        self.worksheet = None
        self.row = None
        self.col = None


    def assembleDemandData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        demand_store = pd.HDFStore(self.demand_file)
        
        population = demand_store.select('countyPop')
        acs        = demand_store.select('countyACS')
        hu         = demand_store.select('countyHousingUnits')
        employment = demand_store.select('countyEmp')
        lodesWAC   = demand_store.select('lodesWAC')
        lodesRAC   = demand_store.select('lodesRAC')
        lodesOD    = demand_store.select('lodesOD')
        autoOpCost = demand_store.select('autoOpCost')
        tolls      = demand_store.select('tollCost')
        parkingCost= demand_store.select('parkingCost')
        transitFare= demand_store.select('transitFare')

        demand_store.close()
        
        # start with the population, which has the longest time-series, 
        # and join all the others with the month being equivalent
        df = population
        df = pd.merge(df, acs, how='left', on=['MONTH'], sort=True, suffixes=('', '_ACS')) 
        df = pd.merge(df, hu, how='left', on=['MONTH'], sort=True, suffixes=('', '_HU')) 
        df = pd.merge(df, employment, how='left', on=['MONTH'], sort=True, suffixes=('', '_QCEW')) 
        df = pd.merge(df, lodesWAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_WAC')) 
        df = pd.merge(df, lodesRAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_RAC')) 
        df = pd.merge(df, lodesOD, how='left', on=['MONTH'], sort=True, suffixes=('', '_OD')) 
        df = pd.merge(df, autoOpCost, how='left', on=['MONTH'], sort=True, suffixes=('', '_AOP')) 
        df = pd.merge(df, tolls, how='left', on=['MONTH'], sort=True, suffixes=('', '_TOLL')) 
        df = pd.merge(df, parkingCost, how='left', on=['MONTH'], sort=True, suffixes=('', '_PARK')) 
        df = pd.merge(df, transitFare, how='left', on=['MONTH'], sort=True, suffixes=('', '_FARE')) 
                
        return df

        
    def writeDemandReport(self, xlsfile, comments=None):
        '''
        Writes a drivers of demand for all months to the specified excel file.        
        '''    
        
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]
        sheetName = 'Drivers of Demand'
 
        # establish the writer        
        self.writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        
                                
        # get the actual data
        df = self.assembleDemandData()    
                    
        # Write the month as the column headers
        months = df[['MONTH']]
        months.T.to_excel(self.writer, sheet_name=sheetName,  
                                startrow=10, startcol=7, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
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
        worksheet.write(1, 1, 'San Francisco Drivers of Demand Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, 'San Francisco County')
        worksheet.write(5, 2, 'Temporal Resolution: ')
        worksheet.write(5, 3, 'Monthly')
        worksheet.write(6, 2, 'Report Generated on: ')
        worksheet.write(6, 3, timestring)
        worksheet.write(7, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
            
        # Use formulas to calculate the differences
        self.writeSystemValues(df, months, sheetName)
        self.writeSystemDifferenceFormulas(months, sheetName)
        self.writeSystemPercentDifferenceFormulas(months, sheetName)    
            
        # freeze so we can see what's happening
        worksheet.freeze_panes(0, 7)
            
        self.writer.save()
    

    def writeSystemValues(self, df, months, sheetName):
        '''
        Writes the main system values to the worksheet. 
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
        
        df['EmpPerHU'] = 1.0 * df['TOTEMP'] / df['UNITS']
        self.write_row(label='Employees per Housing Unit', data=df[['EmpPerHU']], 
            source='QCEW/Planning Dept', tempRes='Monthly', geogRes='Block', format=dec_format)

        df['EmpPerWorker'] = 1.0 * df['TOTEMP'] / df['WORKERS_RAC']
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
            


    def writeSystemDifferenceFormulas(self, months, sheetName): 
        '''
        Adds formulas to the system worksheet to calculate differences
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



    def writeSystemPercentDifferenceFormulas(self, months, sheetName): 
        '''
        Adds formulas to the system worksheet to calculate percent differences
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

    