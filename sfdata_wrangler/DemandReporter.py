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
        hu         = demand_store.select('countyHousingCompletions')
        employment = demand_store.select('countyEmp')
        lodesWAC   = demand_store.select('lodesWAC')
        lodesRAC   = demand_store.select('lodesRAC')
        lodesOD    = demand_store.select('lodesOD')
        fuelPrice  = demand_store.select('fuelPrice')

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
        df = pd.merge(df, fuelPrice, how='left', on=['MONTH'], sort=True, suffixes=('', '_FP')) 
                
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
        worksheet.set_column(0, 1, 3)
        worksheet.set_column(2, 2, 45)
        worksheet.set_column(3, 5, 15)
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
        #self.writeSystemDifferenceFormulas(months, sheetName)
        #self.writeSystemPercentDifferenceFormulas(months, sheetName)    
            
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
        worksheet.write(9, 6, 'Values', bold)
        worksheet.write(10, 3, 'Source', bold)        
        worksheet.write(10, 4, 'Temporal Res', bold)        
        worksheet.write(10, 5, 'Geographic Res', bold)        
        worksheet.write(10, 6, 'Trend', bold)        
        
        self.set_position(self.writer, worksheet, 11, 2)
        
        # POPULATION & HOUSEHOLDS
        worksheet.write(self.row, 1, 'Population & Households', bold)
        self.row += 1
        
        self.write_row(label='Population', data=df[['POP']], 
            source='Census PopEst', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Households', data=df[['HH']], 
            source='ACS', tempRes='Annual', geogRes='County', format=int_format)

        self.write_row(label='Housing Units', data=df[['NETUNITS']], 
            source='Planning Dept', tempRes='Date', geogRes='Address', format=int_format)

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

        self.write_row(label='Mean Household Income (2010$)', data=df[['MEAN_HHINC_2010USD']], 
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

        # INTRA-COUNTY WORKERS
        worksheet.write(self.row, 1, 'Intra-County Workers', bold)
        self.row += 1
        
        self.write_row(label='Live & Work in Same County', data=df[['SFWORKERS']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Live & Work in Same County, earning $0-15k', data=df[['SFWORKERS_EARN0_15']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Live & Work in Same County, earning $15-40k', data=df[['SFWORKERS_EARN15_40']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        self.write_row(label='Live & Work in Same County, earning $40k+', data=df[['SFWORKERS_EARN40P']], 
            source='LODES OD/QCEW', tempRes='Annual/Monthly', geogRes='Block', format=int_format)
            
        # COSTS
        worksheet.write(self.row, 1, 'Costs', bold)
        self.row += 1
        
        self.write_row(label='Average Fuel Price (2010$)', data=df[['FUEL_PRICE_2010USD']], 
            source='EIA', tempRes='Monthly', geogRes='MSA', format=cent_format)
            
        self.write_row(label='Average Fleet Efficiency (mpg)', data=df[[]], 
            source='EIA', tempRes='Annual', geogRes='County', format=dec_format)
            
        self.write_row(label='Average Auto Operating Cost (2010$/mile)', data=df[[]], 
            source='IRS', tempRes='Annual', geogRes='US', format=cent_format)
            
        self.write_row(label='Average Daily Parking Cost (2010$)', data=df[[]], 
            source='Unknown', tempRes='Annual', geogRes='County', format=cent_format)
            
        self.write_row(label='Tolls: Bay Bridge (2010$)', data=df[[]], 
            source='BATA', tempRes='Monthly', geogRes='County', format=cent_format)
            
        self.write_row(label='Tolls: Golden Gate Bridge (2010$)', data=df[[]], 
            source='GGBA', tempRes='Monthly', geogRes='County', format=cent_format)
            
        self.write_row(label='Transit Fares: MUNI Cash Fare (2010$)', data=df[[]], 
            source='SFMTA', tempRes='Monthly', geogRes='County', format=cent_format)
            
        self.write_row(label='Transit Fares: MUNI Average Fare (2010$)', data=df[[]], 
            source='MTC', tempRes='Annual', geogRes='County', format=cent_format)
            
        self.write_row(label='Transit Fares: BART Freemont to Embarcadero (2010$)', data=df[[]], 
            source='BART', tempRes='Monthly', geogRes='County', format=cent_format)
            
        self.write_row(label='Transit Fares: BART Average Fare (2010$)', data=df[[]], 
            source='MTC', tempRes='Annual', geogRes='County', format=cent_format)
            
        self.write_row(label='Consumer Price Index', data=df[['CPI']], 
            source='BLS', tempRes='Monthly', geogRes='US City Avg', format=int_format)

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
            self.write_row(label=label, data=df[[key]], 
                source='ACS', tempRes='Annual', geogRes='County', format=percent_format)


        # MODE SHARES BY SEGMENT   
        groups = [('JTW_EARN0_15_',    'Workers earning $0-15k: '),
                  ('JTW_EARN15_50_',   'Workers earning $15-50k: '),
                  ('JTW_EARN50P_',     'Workers earning $50k+: '),
                  ('JTW_0VEH_',        'Workers with 0 vehicles: ')]

        worksheet.write(self.row, 1, 'Commute Mode Shares by Segment', bold)
        self.row += 1

        for group, groupName in groups: 
            for mode, modeName in modes: 

                key = group + mode + '_SHARE'
                label = groupName + modeName

                self.write_row(label=label, data=df[[key]], 
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format)

            

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

    