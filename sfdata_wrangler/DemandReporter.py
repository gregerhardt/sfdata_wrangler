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


    def assembleDemandData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        demand_store = pd.HDFStore(self.demand_file)
          
        population = demand_store.select('countyPop')
        acs        = demand_store.select('countyACS')
        employment = demand_store.select('countyEmp')
        lodesRAC   = demand_store.select('lodesRAC')
        lodesWAC   = demand_store.select('lodesWAC')
        lodesOD    = demand_store.select('lodesOD')
        fuelPrice  = demand_store.select('fuelPrice')

        demand_store.close()
        
        # start with the population, which has the longest time-series, 
        # and join all the others with the month being equivalent
        df = population
        df = pd.merge(df, acs, how='left', on=['MONTH'], sort=True, suffixes=('', '_ACS')) 
        df = pd.merge(df, employment, how='left', on=['MONTH'], sort=True, suffixes=('', '_QCEW')) 
        df = pd.merge(df, lodesRAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_RAC')) 
        df = pd.merge(df, lodesWAC, how='left', on=['MONTH'], sort=True, suffixes=('', '_WAC')) 
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
                                startrow=10, startcol=6, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})        
            
        # set the column widths
        worksheet.set_column(0, 1, 3)
        worksheet.set_column(2, 2, 45)
        worksheet.set_column(3, 3, 15)
        worksheet.set_column(4, 4, 15)
        worksheet.set_column(5, 5, 25)                    
            
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
        worksheet.freeze_panes(0, 6)
            
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
        worksheet.write(10, 4, 'Resolution', bold)        
        worksheet.write(10, 5, 'Trend', bold)        
        
        # POPULATION & HOUSEHOLDS
        worksheet.write(11, 1, 'Population & Households', bold)
        
        self.write_row(worksheet, 12, 2, 
            label='Population', data=df[['POP']], 
            source='Census PopEst', res='Annual', format=int_format)

        self.write_row(worksheet, 13, 2, 
            label='Households', data=df[['HH']], 
            source='ACS', res='Annual', format=int_format)

        self.write_row(worksheet, 14, 2, 
            label='Households, Income $0-15k', data=df[['HH_INC0_15']], 
            source='ACS', res='Annual', format=int_format)

        self.write_row(worksheet, 15, 2, 
            label='Households, Income $15-50k', data=df[['HH_INC15_50']], 
            source='ACS', res='Annual', format=int_format)

        self.write_row(worksheet, 16, 2, 
            label='Households, Income $50-100k', data=df[['HH_INC50_100']], 
            source='ACS', res='Annual', format=int_format)

        self.write_row(worksheet, 17, 2, 
            label='Households, Income $100k+', data=df[['HH_INC100P']], 
            source='ACS', res='Annual', format=int_format)

        self.write_row(worksheet, 18, 2, 
            label='Households, 0 Vehicles', data=df[['HH_0VEH']], 
            source='ACS', res='Annual', format=int_format)
        
        self.write_row(worksheet, 19, 2, 
            label='Median Household Income (2010$)', data=df[['MEDIAN_HHINC_2010USD']], 
            source='ACS', res='Annual', format=dollar_format)

        self.write_row(worksheet, 20, 2, 
            label='Mean Household Income (2010$)', data=df[['MEAN_HHINC_2010USD']], 
            source='ACS', res='Annual', format=dollar_format)
            
            
        # POPULATION & HOUSEHOLDS
        worksheet.write(21, 1, 'Workers (at home location)', bold)
        
        self.write_row(worksheet, 22, 2, 
            label='Workers', data=df[['WORKERS_RAC']], 
            source='LODES RAC/QCEW', res='Annual/Monthly', format=int_format)
            
        self.write_row(worksheet, 23, 2, 
            label='Workers, earning $0-15k', data=df[['WORKERS_EARN0_15']], 
            source='LODES RAC/QCEW', res='Annual/Monthly', format=int_format)

        self.write_row(worksheet, 24, 2, 
            label='Workers, earning $15-40k', data=df[['WORKERS_EARN15_40']], 
            source='LODES RAC/QCEW', res='Annual/Monthly', format=int_format)
            
        self.write_row(worksheet, 25, 2, 
            label='Workers, earning $40k+', data=df[['WORKERS_EARN40P']], 
            source='LODES RAC/QCEW', res='Annual/Monthly', format=int_format)
        
            
        # EMPLOYMENT
        worksheet.write(26, 1, 'Employment (at work location)', bold)
        
        self.write_row(worksheet, 27, 2, 
            label='Total Employment', data=df[['TOTEMP']], 
            source='QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 28, 2, 
            label='Retail Employment', data=df[['RETAIL_EMP']], 
            source='QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 29, 2, 
            label='Education and Health Employment', data=df[['EDHEALTH_EMP']], 
            source='QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 30, 2, 
            label='Leisure Employment', data=df[['LEISURE_EMP']], 
            source='QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 31, 2, 
            label='Other Employment', data=df[['OTHER_EMP']], 
            source='QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 32, 2, 
            label='Employees, earning $0-15k', data=df[['EMP_EARN0_15']], 
            source='LODES WAC/QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 33, 2, 
            label='Employees, earning $15-40k', data=df[['EMP_EARN15_40']], 
            source='LODES WAC/QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 34, 2, 
            label='Employees, earning $40k+', data=df[['EMP_EARN40P']], 
            source='LODES WAC/QCEW', res='Monthly', format=int_format)
            
        self.write_row(worksheet, 35, 2, 
            label='Average monthly earnings (2010$)', data=df[['AVG_MONTHLY_EARNINGS_USD2010']], 
            source='QCEW', res='Monthly', format=dollar_format)

        # INTRA-COUNTY WORKERS
        worksheet.write(36, 1, 'Intra-County Workers', bold)
        
        self.write_row(worksheet, 37, 2, 
            label='Live & Work in Same County', data=df[['SFWORKERS']], 
            source='LODES OD/QCEW', res='Annual/Monthly', format=int_format)
            
        self.write_row(worksheet, 38, 2, 
            label='Live & Work in Same County, earning $0-15k', data=df[['SFWORKERS_EARN0_15']], 
            source='LODES OD/QCEW', res='Annual/Monthly', format=int_format)
            
        self.write_row(worksheet, 39, 2, 
            label='Live & Work in Same County, earning $15-40k', data=df[['SFWORKERS_EARN15_40']], 
            source='LODES OD/QCEW', res='Annual/Monthly', format=int_format)
            
        self.write_row(worksheet, 40, 2, 
            label='Live & Work in Same County, earning $40k+', data=df[['SFWORKERS_EARN40P']], 
            source='LODES OD/QCEW', res='Annual/Monthly', format=int_format)
            
        # COSTS
        worksheet.write(41, 1, 'Costs', bold)
        
        self.write_row(worksheet, 42, 2, 
            label='Average Fuel Price (2010$)', data=df[['FUEL_PRICE_2010USD']], 
            source='EIA', res='Monthly', format=cent_format)
            
        self.write_row(worksheet, 43, 2, 
            label='Average Fleet Efficiency (mpg)', data=df[[]], 
            source='EIA', res='Annual', format=dec_format)
            
        self.write_row(worksheet, 44, 2, 
            label='Average Auto Operating Cost (2010$/mile)', data=df[[]], 
            source='IRS', res='Annual', format=cent_format)
            
        self.write_row(worksheet, 45, 2, 
            label='Average Daily Parking Cost (2010$)', data=df[[]], 
            source='Unknown', res='Annual', format=cent_format)
            
        self.write_row(worksheet, 46, 2, 
            label='Tolls: Bay Bridge (2010$)', data=df[[]], 
            source='BATA', res='Monthly', format=cent_format)
            
        self.write_row(worksheet, 47, 2, 
            label='Tolls: Golden Gate Bridge (2010$)', data=df[[]], 
            source='GGBA', res='Monthly', format=cent_format)
            
        self.write_row(worksheet, 48, 2, 
            label='Transit Fares: MUNI Cash Fare (2010$)', data=df[[]], 
            source='SFMTA', res='Monthly', format=cent_format)
            
        self.write_row(worksheet, 49, 2, 
            label='Transit Fares: MUNI Average Fare (2010$)', data=df[[]], 
            source='MTC', res='Annual', format=cent_format)
            
        self.write_row(worksheet, 50, 2, 
            label='Transit Fares: BART Freemont to Embarcadero (2010$)', data=df[[]], 
            source='BART', res='Monthly', format=cent_format)
            
        self.write_row(worksheet, 51, 2, 
            label='Transit Fares: BART Average Fare (2010$)', data=df[[]], 
            source='MTC', res='Annual', format=cent_format)
            
        self.write_row(worksheet, 52, 2, 
            label='Consumer Price Index', data=df[['CPI']], 
            source='BLS', res='Monthly', format=int_format)

        # MODE SHARES
        worksheet.write(53, 1, 'Commute Mode Shares', bold)
        
        self.write_row(worksheet, 54, 2, 
            label='Drive-Alone', data=df[['JTW_DA_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 55, 2, 
            label='Carpool', data=df[['JTW_SR_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 56, 2, 
            label='Transit', data=df[['JTW_TRANSIT_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 57, 2, 
            label='Walk', data=df[['JTW_WALK_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 58, 2, 
            label='Taxi, bike, other', data=df[['JTW_OTHER_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 59, 2, 
            label='Work at home', data=df[['JTW_HOME_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
            
        self.write_row(worksheet, 60, 2, 
            label='Workers earning $0-15k: Drive-Alone', data=df[['JTW_EARN0_15_DA_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 61, 2, 
            label='Workers earning $0-15k: Carpool', data=df[['JTW_EARN0_15_SR_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 62, 2, 
            label='Workers earning $0-15k: Transit', data=df[['JTW_EARN0_15_TRANSIT_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 63, 2, 
            label='Workers earning $0-15k: Walk', data=df[['JTW_EARN0_15_WALK_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 64, 2, 
            label='Workers earning $0-15k: Taxi, bike, other', data=df[['JTW_EARN0_15_OTHER_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 65, 2, 
            label='Workers earning $0-15k: Work at home', data=df[['JTW_EARN0_15_HOME_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
            
        self.write_row(worksheet, 66, 2, 
            label='Workers earning $15-50k: Drive-Alone', data=df[['JTW_EARN15_50_DA_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 67, 2, 
            label='Workers earning $15-50k: Carpool', data=df[['JTW_EARN15_50_SR_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 68, 2, 
            label='Workers earning $15-50k: Transit', data=df[['JTW_EARN15_50_TRANSIT_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 69, 2, 
            label='Workers earning $15-50k: Walk', data=df[['JTW_EARN15_50_WALK_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 70, 2, 
            label='Workers earning $15-50k: Taxi, bike, other', data=df[['JTW_EARN15_50_OTHER_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 71, 2, 
            label='Workers earning $15-50k: Work at home', data=df[['JTW_EARN15_50_HOME_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
            
        self.write_row(worksheet, 72, 2, 
            label='Workers earning $50k+: Drive-Alone', data=df[['JTW_EARN50P_DA_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 73, 2, 
            label='Workers earning $50k+: Carpool', data=df[['JTW_EARN50P_SR_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 74, 2, 
            label='Workers earning $50k+: Transit', data=df[['JTW_EARN50P_TRANSIT_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 75, 2, 
            label='Workers earning $50k+: Walk', data=df[['JTW_EARN50P_WALK_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 76, 2, 
            label='Workers earning $50k+: Taxi, bike, other', data=df[['JTW_EARN50P_OTHER_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 77, 2, 
            label='Workers earning $50k+: Work at home', data=df[['JTW_EARN50P_HOME_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
            
        self.write_row(worksheet, 78, 2, 
            label='Workers with 0 vehicles: Drive-Alone', data=df[['JTW_0VEH_DA_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 79, 2, 
            label='Workers with 0 vehicles: Carpool', data=df[['JTW_0VEH_SR_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 80, 2, 
            label='Workers with 0 vehicles: Transit', data=df[['JTW_0VEH_TRANSIT_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 81, 2, 
            label='Workers with 0 vehicles: Walk', data=df[['JTW_0VEH_WALK_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 82, 2, 
            label='Workers with 0 vehicles: Taxi, bike, other', data=df[['JTW_0VEH_OTHER_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
        self.write_row(worksheet, 83, 2, 
            label='Workers with 0 vehicles: Work at home', data=df[['JTW_0VEH_HOME_SHARE']], 
            source='ACS', res='Annual', format=percent_format)
            
            
    def write_row(self, worksheet, r, c, label, source, res, data, format, sparkline=True):
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
        worksheet.write(r, c, label)
        worksheet.write(r, c+1, source)
        worksheet.write(r, c+2, res)

        # data
        worksheet.set_row(r, None, format) 
        data.T.to_excel(self.writer, sheet_name=worksheet.get_name(), 
                            startrow=r, startcol=c+4, header=False, index=False)
        
        # sparkline
        if sparkline: 
            cell = xl_rowcol_to_cell(r, c+3)
            data_range = xl_rowcol_to_cell(r, c+4) + ':' + xl_rowcol_to_cell(r, c+4+len(data)+1)
            worksheet.add_sparkline(cell, {'range': data_range})   
    