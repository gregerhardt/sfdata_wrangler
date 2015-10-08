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
import glob
import os


def getFiscalYear(month):
    '''
    gets the fiscal year given a month
    '''
    if month.month<=6: 
        return month.year
    else:
        return month.year + 1

    
class MultiModalHelper():
    """ 
    Class to create multi-modal performance data. 
    
    """
    
    ANNUAL_TRANSIT_YEARS = [2000,2014]


    def __init__(self):
        '''
        Constructor. 

        '''   


    def processAnnualTransitData(self, annualTransitDir, cpiFile, outfile): 
        """ 
        Processes the annual multi-modal data.  
        
        annualTransitDir - file containing data from Transit Statistical Summaries
        cpiFile - contains consumer price index data
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/transitAnnual' in keys: 
            outstore.remove('transitAnnual')

        # the output dataframe
        annual = pd.DataFrame({'FISCAL_YEAR': range(self.ANNUAL_TRANSIT_YEARS[0], self.ANNUAL_TRANSIT_YEARS[1]+1)})

        # get annual data from the transit statistical summaries
        transitSpecs = [('ServiceMiles', 'SERVMILES_'), 
                        ('Passengers', 'PASSENGERS_'), 
                        ('FareboxRevenue', 'FAREBOX_'), 
                        ('AvgWeekdayRidership', 'AVG_WEEKDAY_RIDERSHIP_')
                        ]
        
        # get the data
        for fileLabel, colLabel in transitSpecs: 
            filename = annualTransitDir + '/TransitAnnual' + fileLabel + '.csv'
            df = pd.read_csv(filename)
            
            # get the total bus
            df['MUNI_BUS'] = df['MUNI_MOTORBUS'] + df['MUNI_TROLLEYBUS']
            
            # set the column names
            colNames = {}
            for oldName in df.columns: 
                if oldName != 'PeriodStart': 
                    newName = colLabel + oldName
                    colNames[oldName] = newName
            df = df.rename(columns=colNames)
            
            df['MONTH'] = df['PeriodStart'].apply(pd.to_datetime)
            df['FISCAL_YEAR'] = df['MONTH'].apply(lambda x: x.year+1)
            
            annual = pd.merge(annual, df, how='left', on=['FISCAL_YEAR'], sort=True, suffixes=('', colLabel))
        
        # append to the output store
        outstore.append('transitAnnual', annual, data_columns=True)
        outstore.close()


    def processMonthlyTransitData(self, cpiFile, outfile): 
        """ 
        Converts the annual multi-modal data to monthly measures.   

        cpiFile - contains consumer price index data
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/transitMonthly' in keys: 
            outstore.remove('transitMonthly')

        # get the annual data
        annual = outstore.select('transitAnnual')
        
        # expand to a monthly, using backfill to keep same values for whole year
        monthly = annual.set_index(pd.DatetimeIndex(annual['MONTH']))
        monthly = monthly.resample('M', fill_method='ffill')
        monthly['MONTH'] = monthly.index
        monthly['MONTH'] = monthly['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # scale annual values to monthly values
        # by default, use 1/12th.  Use better information if we have it...
        defaultFactors = [('SERVMILES_', 1.0/12.0), 
                         ('PASSENGERS_', 1.0/12.0), 
                         ('FAREBOX_', 1.0/12.0), 
                         ('AVG_WEEKDAY_RIDERSHIP_', 1.0)
                         ]
        modes = ['BART', 'CALTRAIN', 'MUNI_BUS', 'MUNI_MOTORBUS', 'MUNI_TROLLEYBUS', 'MUNI_CC', 'MUNI_RAIL']
        for colLabel, factor in defaultFactors: 
            for mode in modes: 
                col = colLabel + mode
                monthly[col] = monthly[col] * factor
        
        # append to the output store
        outstore.append('transitMonthly', monthly, data_columns=True)
        outstore.close()


    def processTransitFares(self, cashFareFile, cpiFile, outfile): 
        """ 
        Processes the cash transit fares into a monthly list format. 
        
        cashFareFile - file containing the input fares in nominal dollars
        cpiFile  - inflation factors
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/transitFare' in keys: 
            outstore.remove('transitFare')
        if '/transitFareAnnual' in keys: 
            outstore.remove('transitFareAnnual')
        
        # get the data and expand it to monthly
        df = pd.read_csv(cashFareFile)
                
        # expand to a monthly, using backfill to keep same rate until it changes
        df = df.set_index(pd.DatetimeIndex(df['PeriodStart']))
        df = df.resample('M', fill_method='ffill')
        df['MONTH'] = df.index
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # determine the fiscal year averages
        df['FISCAL_YEAR'] = df['MONTH'].apply(getFiscalYear)   
        df_fy = df.groupby('FISCAL_YEAR').agg('mean') 
        df_fy['FISCAL_YEAR'] = df_fy.index
        df_fy['MONTH'] = df_fy['FISCAL_YEAR'].apply(lambda x: pd.to_datetime(str(x) + '-01-01'))

        # get and make sure the indices are aligned
        farebox_fy = outstore.get('transitAnnual')
        farebox_fy.index = farebox_fy['FISCAL_YEAR']
        
        # consolidate muni bus and rail        
        # calculate average fare paid
        farebox_fy['FAREBOX_MUNI'] = farebox_fy['FAREBOX_MUNI_BUS'] + farebox_fy['FAREBOX_MUNI_RAIL']
        farebox_fy['PASSENGERS_MUNI'] = farebox_fy['PASSENGERS_MUNI_BUS'] + farebox_fy['PASSENGERS_MUNI_RAIL']
        modes = ['MUNI', 'MUNI_CC', 'BART', 'CALTRAIN']
        for mode in modes: 
            df_fy['AVG_FARE_' + mode] = farebox_fy['FAREBOX_' + mode] / farebox_fy['PASSENGERS_' + mode]
            if mode != 'CALTRAIN': 
                df_fy['FARE_RATIO_' + mode] = df_fy['AVG_FARE_' + mode] / df_fy['CASH_FARE_' + mode]

        # merge back to monthly data, and apply the fare ratios to calculate average fares
        df = pd.merge(df, df_fy, how='left', on='FISCAL_YEAR', sort=True, suffixes=('', '_FY')) 
        for mode in modes: 
            if mode != 'CALTRAIN': 
                df['AVG_FARE_' + mode] = df['CASH_FARE_' + mode] * df['FARE_RATIO_' + mode]
        
        # keep only needed fields
        fields = ['MONTH']
        for mode in modes:
            if mode != 'CALTRAIN': 
                fields = fields + ['CASH_FARE_' + mode]
            fields = fields + ['AVG_FARE_' + mode]
        df = df[fields]

        # adjust for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        df_fy = pd.merge(df_fy, dfcpi, how='left', on=['MONTH'], sort=True)  
        for mode in modes:
            if mode != 'CALTRAIN': 
                df['CASH_FARE_2010USD_' + mode] = df['CASH_FARE_' + mode] * df['CPI_FACTOR']
                df_fy['CASH_FARE_2010USD_' + mode] = df_fy['CASH_FARE_' + mode] * df_fy['CPI_FACTOR']
            df['AVG_FARE_2010USD_' + mode] = df['AVG_FARE_' + mode] * df['CPI_FACTOR']
            df_fy['AVG_FARE_2010USD_' + mode] = df_fy['AVG_FARE_' + mode] * df_fy['CPI_FACTOR']

                
        # append to the output store
        outstore.append('transitFare', df, data_columns=True)
        outstore.append('transitFareAnnual', df_fy, data_columns=True)
        outstore.close()
    
    
    
    def getCPIFactors(self, cpiFile):
        """ 
        Reads CPI numbers and returns a dataframe with the CPI_FACTOR field
        that can be joined by month and used to adjust monetary values by 
        inflation to 2010 US dollars.  
        
        """
        
        # get the CPI and convert to monthly format
        dfcpi = pd.read_excel(cpiFile, sheetname='BLS Data Series', skiprows=10, index_col=0)
        base = dfcpi.at[2010, 'Annual']
        
        dfcpi = dfcpi.drop(['Annual', 'HALF1', 'HALF2'], axis=1)
        dfcpi = dfcpi.stack()
        dfcpi = dfcpi.reset_index()
        dfcpi = dfcpi.rename(columns={
                             'level_1' : 'monthString', 
                             0 : 'CPI'
                             })
                             
        dfcpi['MONTH'] = '01-' + dfcpi['monthString'] + '-' + dfcpi['Year'].astype('string')
        dfcpi['MONTH'] = dfcpi['MONTH'].apply(pd.Timestamp)
        
        dfcpi['CPI_FACTOR'] = base / dfcpi['CPI']
        
        dfcpi = dfcpi[['MONTH', 'CPI', 'CPI_FACTOR']]
        
        return dfcpi
