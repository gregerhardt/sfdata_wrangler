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


def convertToDate(dateString):
    '''
    Converts a string to a date.  
    '''
    date = pd.to_datetime(dateString)
    if date < pd.to_datetime('1990-01-01'): 
        date = pd.NaT
    return date


def convertDateToMonth(date):
    '''
    Given a date, returns the month
    '''
    if pd.isnull(date): 
        return pd.NaT
    else: 
        month = ((pd.to_datetime(date)).to_period('month')).to_timestamp() 
        return month

    
class DemandHelper():
    """ 
    Class to create drivers of demand data: employment, population, fuel cost.
    
    """

    # the range of years for these data files
    POP_EST_YEARS = [2000,2014]
    HU_YEARS      = [2001,2012]
    ACS_YEARS     = [2005,2013]
    LODES_YEARS   = [2002,2013]
    
    def __init__(self):
        '''
        Constructor. 

        '''   
    
    
    def processCensusPopulationEstimates(self, pre2010File, post2010File, fips, outfile): 
        """ 
        Reads the Census annual population estimates, which are published
        at a county level, interpolates them to monthly values, and writes
        them into a consolidated file.  
        
        pre2010File - file containing intercensal (retrospective) population 
                      estimates between 2000 and 2010
        post2010File - file containing postcensal population estimates
        fips     - the  FIPS codes to process, as string
        outfile - the HDF output file to write to
        
        """
                
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyPop' in keys: 
            outstore.remove('countyPop')

        # create the output file for annual data
        annual = pd.DataFrame({'YEAR': range(self.POP_EST_YEARS[0], self.POP_EST_YEARS[1]+1)})
        annual['POP'] = 0
        annual.index = range(self.POP_EST_YEARS[0], self.POP_EST_YEARS[1]+1)
        
        # get raw data, pre-2010, and copy to annual file
        pre2010_raw = pd.read_csv(pre2010File)
        fips_state = fips[:2]
        fips_county = fips[2:]
        pre2010_raw = pre2010_raw[(pre2010_raw['STATE']==int(fips_state)) 
                                & (pre2010_raw['COUNTY']==int(fips_county))]
        pre2010_raw.index = range(0, len(pre2010_raw))
        
        for year in range(2000, 2010): 
            annual.at[year,'POP'] = pre2010_raw.at[0, 'POPESTIMATE' + str(year)]
            
        
        # get raw data, post-2010
        post2010_raw = pd.read_csv(post2010File, skiprows=1)
        post2010_raw = post2010_raw[post2010_raw['Id2']==int(fips)]
        post2010_raw.index = range(0, len(post2010_raw))
        
        for year in range(2010, self.POP_EST_YEARS[1]+1): 
            annual.at[year,'POP'] = post2010_raw.at[0, 'Population Estimate (as of July 1) - ' + str(year)]

        # convert data to monthly
        monthly = self.convertAnnualToMonthly(annual)
                        
        # append to the output store
        outstore.append('countyPop', monthly, data_columns=True)
        outstore.close()



    # a list of output field and inputfield tuples for each table
    ACS_EQUIV = {'B01003' : [('POP_ACS', 'Estimate; Total')
                            ], 
                 'DP03'   : [('HH',           'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Total households'),
                             ('WORKERS',      'Estimate; EMPLOYMENT STATUS - In labor force - Civilian labor force - Employed'), 
                             ('MEDIAN_HHINC', 'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Median household income (dollars)'), 
                             ('MEAN_HHINC',   'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Mean household income (dollars)'), 
                             ('HH_INC0_15',  ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Less than $10,000',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $10,000 to $14,999']), 
                             ('HH_INC15_50', ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $15,000 to $24,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $25,000 to $34,999', 
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $35,000 to $49,999']), 
                             ('HH_INC50_100',['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $50,000 to $74,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $75,000 to $99,999']), 
                             ('HH_INC100P',  ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $100,000 to $149,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $150,000 to $199,999', 
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $200,000 or more'])
                            ], 
                 'B08203' : [('HH_0VEH',      'Estimate; No vehicle available'),
                             ('HH_1VEH',      'Estimate; 1 vehicle available'), 
                             ('HH_2PVEH',    ['Estimate; 2 vehicles available',
                                              'Estimate; 3 vehicles available', 
                                              'Estimate; 4 vehicles available'])
                            ],
                 'B08119' : [('JTW_DA',       'Estimate; Car, truck, or van - drove alone:'),
                             ('JTW_SR',       'Estimate; Car, truck, or van - carpooled:'), 
                             ('JTW_TRANSIT',  'Estimate; Public transportation (excluding taxicab):'), 
                             ('JTW_WALK',     'Estimate; Walked:'), 
                             ('JTW_OTHER',    'Estimate; Taxicab, motorcycle, bicycle, or other means:'), 
                             ('JTW_HOME',     'Estimate; Worked at home:'),                              
                             
                             ('JTW_EARN0_15_DA',      ['Estimate; Car, truck, or van - drove alone: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - drove alone: - $10,000 to $14,999']),
                             ('JTW_EARN0_15_SR',      ['Estimate; Car, truck, or van - carpooled: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - carpooled: - $10,000 to $14,999']),
                             ('JTW_EARN0_15_TRANSIT', ['Estimate; Public transportation (excluding taxicab): - $1 to $9,999 or loss', 
                                                       'Estimate; Public transportation (excluding taxicab): - $10,000 to $14,999']),
                             ('JTW_EARN0_15_WALK',    ['Estimate; Walked: - $1 to $9,999 or loss', 
                                                       'Estimate; Walked: - $10,000 to $14,999']),
                             ('JTW_EARN0_15_OTHER',   ['Estimate; Taxicab, motorcycle, bicycle, or other means: - $1 to $9,999 or loss', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $10,000 to $14,999']),
                             ('JTW_EARN0_15_HOME',    ['Estimate; Worked at home: - $1 to $9,999 or loss', 
                                                       'Estimate; Worked at home: - $10,000 to $14,999']),
                             
                             ('JTW_EARN15_50_DA',     ['Estimate; Car, truck, or van - drove alone: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $35,000 to $49,999']),
                             ('JTW_EARN15_50_SR',     ['Estimate; Car, truck, or van - carpooled: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $35,000 to $49,999']),
                             ('JTW_EARN15_50_TRANSIT',['Estimate; Public transportation (excluding taxicab): - $15,000 to $24,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $25,000 to $34,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $35,000 to $49,999']),
                             ('JTW_EARN15_50_WALK',   ['Estimate; Walked: - $15,000 to $24,999', 
                                                       'Estimate; Walked: - $25,000 to $34,999',
                                                       'Estimate; Walked: - $35,000 to $49,999']),
                             ('JTW_EARN15_50_OTHER',  ['Estimate; Taxicab, motorcycle, bicycle, or other means: - $15,000 to $24,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $25,000 to $34,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $35,000 to $49,999']),
                             ('JTW_EARN15_50_HOME',   ['Estimate; Worked at home: - $15,000 to $24,999', 
                                                       'Estimate; Worked at home: - $25,000 to $34,999',
                                                       'Estimate; Worked at home: - $35,000 to $49,999']),
                             
                             ('JTW_EARN50P_DA',       ['Estimate; Car, truck, or van - drove alone: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $65,000 to $74,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $75,000 or more']),
                             ('JTW_EARN50P_SR',       ['Estimate; Car, truck, or van - carpooled: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $65,000 to $74,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $75,000 or more']),
                             ('JTW_EARN50P_TRANSIT',  ['Estimate; Public transportation (excluding taxicab): - $50,000 to $64,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $65,000 to $74,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $75,000 or more']),
                             ('JTW_EARN50P_WALK',     ['Estimate; Walked: - $50,000 to $64,999', 
                                                       'Estimate; Walked: - $65,000 to $74,999',
                                                       'Estimate; Walked: - $75,000 or more']),
                             ('JTW_EARN50P_OTHER',    ['Estimate; Taxicab, motorcycle, bicycle, or other means: - $50,000 to $64,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $65,000 to $74,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $75,000 or more']),
                             ('JTW_EARN50P_HOME',     ['Estimate; Worked at home: - $50,000 to $64,999', 
                                                       'Estimate; Worked at home: - $65,000 to $74,999',
                                                       'Estimate; Worked at home: - $75,000 or more']),
                            ],
                 'B08141' : [('JTW_0VEH_DA',           'Estimate; Car, truck, or van - drove alone: - No vehicle available'),
                             ('JTW_0VEH_SR',           'Estimate; Car, truck, or van - carpooled: - No vehicle available'),
                             ('JTW_0VEH_TRANSIT',      'Estimate; Public transportation (excluding taxicab): - No vehicle available'),
                             ('JTW_0VEH_WALK',         'Estimate; Walked: - No vehicle available'),
                             ('JTW_0VEH_OTHER',        'Estimate; Taxicab, motorcycle, bicycle, or other means: - No vehicle available'),
                             ('JTW_0VEH_HOME',         'Estimate; Worked at home: - No vehicle available'),
                             
                             ('JTW_1VEH_DA',           'Estimate; Car, truck, or van - drove alone: - 1 vehicle available'),
                             ('JTW_1VEH_SR',           'Estimate; Car, truck, or van - carpooled: - 1 vehicle available'),
                             ('JTW_1VEH_TRANSIT',      'Estimate; Public transportation (excluding taxicab): - 1 vehicle available'),
                             ('JTW_1VEH_WALK',         'Estimate; Walked: - 1 vehicle available'),
                             ('JTW_1VEH_OTHER',        'Estimate; Taxicab, motorcycle, bicycle, or other means: - 1 vehicle available'),
                             ('JTW_1VEH_HOME',         'Estimate; Worked at home: - 1 vehicle available'),
                             
                             ('JTW_2PVEH_DA',         ['Estimate; Car, truck, or van - drove alone: - 2 vehicles available', 
                                                       'Estimate; Car, truck, or van - drove alone: - 3 vehicles available',
                                                       'Estimate; Car, truck, or van - drove alone: - 4 vehicles available',
                                                       'Estimate; Car, truck, or van - drove alone: - 5 vehicles available']),
                             ('JTW_2PVEH_SR',         ['Estimate; Car, truck, or van - carpooled: - 2 vehicles available', 
                                                       'Estimate; Car, truck, or van - carpooled: - 3 vehicles available',
                                                       'Estimate; Car, truck, or van - carpooled: - 4 vehicles available',
                                                       'Estimate; Car, truck, or van - carpooled: - 5 vehicles available']),
                             ('JTW_2PVEH_TRANSIT',    ['Estimate; Public transportation (excluding taxicab): - 2 vehicles available', 
                                                       'Estimate; Public transportation (excluding taxicab): - 3 vehicles available',
                                                       'Estimate; Public transportation (excluding taxicab): - 4 vehicles available',
                                                       'Estimate; Public transportation (excluding taxicab): - 5 vehicles available']),
                             ('JTW_2PVEH_WALK',       ['Estimate; Walked: - 2 vehicles available', 
                                                       'Estimate; Walked: - 3 vehicles available',
                                                       'Estimate; Walked: - 4 vehicles available',
                                                       'Estimate; Walked: - 5 vehicles available']),
                             ('JTW_2PVEH_OTHER',      ['Estimate; Taxicab, motorcycle, bicycle, or other means: - 2 vehicles available', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 3 vehicles available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 4 vehicles available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 5 vehicles available']),
                             ('JTW_2PVEH_HOME',       ['Estimate; Worked at home: - 2 vehicles available', 
                                                       'Estimate; Worked at home: - 3 vehicles available',
                                                       'Estimate; Worked at home: - 4 vehicles available',
                                                       'Estimate; Worked at home: - 5 vehicles available']),
                            ]
                }


    def processACSData(self, inputDir, fips, cpiFile, outfile): 
        """ 
        Reads raw ACS data and converts it to a clean list format. 
        
        inputDir - directory containing raw data files
        fips     - the  FIPS codes to process, as string
        outfile  - the HDF output file to write to
        
        """
        fips = int(fips)
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyACS' in keys: 
            outstore.remove('countyACS')
        
        # create the output file for annual data
        years = range(self.ACS_YEARS[0], self.ACS_YEARS[1]+1)
        annual = pd.DataFrame({'YEAR': years})
        annual.index = years
        
        # loop through the tables and get the data
        for table, fields in self.ACS_EQUIV.iteritems():
            
            # initialize the output container
            for outfield, infields in fields: 
                annual[outfield] = np.NaN
                
            # open the table specific to each year
            for year in years: 
                pattern = inputDir + '/' + table + '/ACS_' + str(year)[2:] + '*_with_ann.csv'
                infiles = glob.glob(pattern)
                
                if len(infiles)!=1: 
                    raise IOError('Wrong number of files matching pattern: ' + pattern)
                else: 
                    print infiles[0]
                    df = pd.read_csv(infiles[0], skiprows=1)

                    # get the data relevant to this county
                    # and set the index equal to the fips code
                    df = df[df['Id2']==fips]
                    if 'Population Group' in df.columns: 
                        df = df[df['Population Group']=='Total population']
                    df.index = df['Id2']
                    
                    # normalize the column names
                    colNames = {}
                    for oldName in df.columns: 
                        newName = oldName.replace(str(year), 'YYYY')   
                        newName = newName.replace('Number; ', '')
                        newName = newName.replace('Population 16 years and over - ', '')
                        newName = newName.replace('(IN YYYY INFLATION-ADJUSTED DOLLARS) - Total households -', '(IN YYYY INFLATION-ADJUSTED DOLLARS) -')
                        newName = newName.replace('Total: - ', '')
                        newName = newName.replace('or more vehicles available', 'vehicles available')
                        colNames[oldName] = newName
                    df = df.rename(columns=colNames) 
                    
                    # copy the data over
                    for outfield, infields in fields: 
                        if isinstance(infields, list):  
                            
                            annual.at[year, outfield] = df.at[fips, infields[0]]   
                            for infield in infields[1:]:                                
                            
                                # special case for one problematic table
                                if table=='B08141': 
                                    if not infield in df.columns: 
                                        continue                                
                                
                                annual.at[year, outfield] += float(df.at[fips, infield])
                                
                        else: 
                            annual.at[year, outfield] = df.at[fips, infields]
        
        # convert data to monthly
        monthly = self.convertAnnualToMonthly(annual)
        
        # adjust household incomes for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        monthly = pd.merge(monthly, dfcpi, how='left', on=['MONTH'], sort=True)  
        monthly['MEDIAN_HHINC_2010USD'] = monthly['MEDIAN_HHINC'] * monthly['CPI_FACTOR']
        monthly['MEAN_HHINC_2010USD'] = monthly['MEAN_HHINC'] * monthly['CPI_FACTOR']
        
        # calculate mode shares for journey to work data
        prefixes = ['JTW_', 'JTW_0VEH_', 'JTW_1VEH_', 'JTW_2PVEH_', 'JTW_EARN0_15_', 'JTW_EARN15_50_', 'JTW_EARN50P_']
        modes    = ['DA', 'SR', 'TRANSIT', 'WALK', 'OTHER', 'HOME']
        for prefix in prefixes:
            monthly['total'] = 0.0
            for mode in modes: 
                monthly['total'] = monthly['total'] + monthly[prefix + mode]
            for mode in modes: 
                monthly[prefix + mode + '_SHARE'] = monthly[prefix + mode] / monthly['total']
            monthly.drop('total', axis=1)

        # append to the output store
        outstore.append('countyACS', monthly, data_columns=True)
        outstore.close()
        
        

    def processHousingCompletionsData(self, infile, outfile): 
        """ 
        Reads raw housing completions data and converts it to a clean list format. 
        
        infile   - input csv file
        outfile  - the HDF output file to write to
        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyHousingCompletions' in keys: 
            outstore.remove('countyHousingCompletions')
        
        # read the data, and convert the dates
        df = pd.read_csv(infile)
        df['ACTUAL_DATE'] = df['ACTDT'].apply(convertToDate)
        df['MONTH'] = df['ACTUAL_DATE'].apply(convertDateToMonth)
        
        # split the records between those with an exact date, and 
        # those that only have a year
        dfExact = df[df['MONTH'].apply(pd.notnull)]
        dfNotExact = df[df['MONTH'].apply(pd.isnull)]        
        
        #group and resample to monthly
        dfExact['MONTH'] = dfExact['ACTUAL_DATE'].apply(convertDateToMonth)
        monthlyAgg = dfExact.groupby('MONTH').aggregate(sum)
        monthlyAgg = monthlyAgg.reset_index()
        annualAgg = dfNotExact.groupby('YEAR').aggregate(sum)
        annualAgg = annualAgg.reset_index()
        
        # create the output container
        numYears = self.HU_YEARS[1] - self.HU_YEARS[0] + 1
        months = pd.date_range(str(self.HU_YEARS[0]-1) + '-12-31', 
                periods=12*numYears, freq='M') + pd.DateOffset(days=1)
        dfout = pd.DataFrame({'YEAR': months.year, 'MONTH': months, 'NETUNITS':0})

        # merge the data.  If missing on RHS, then they are zeros. 
        dfout = pd.merge(dfout, monthlyAgg, how='left', on=['MONTH'], sort=True, suffixes=('', '_MONTHLY')) 
        dfout = pd.merge(dfout, annualAgg, how='left', on=['YEAR'], sort=True, suffixes=('', '_ANNUAL')) 
        dfout = dfout.fillna(0)
        
        # accumulate the totals, distributing annual data throughout the year
        dfout['NETUNITS'] += dfout['NETUNITS_MONTHLY']
        dfout['NETUNITS'] += dfout['NETUNITS_ANNUAL'] / 12
        
        dfout = dfout[['YEAR', 'MONTH', 'NETUNITS']]

        dfout.to_csv('c:/temp/housing.csv')

        # write the output
        outstore.append('countyHousingCompletions', dfout, data_columns=True)        
        outstore.close()
        
        

    def processQCEWData(self, inputDir, fips, cpiFile, outfile): 
        """ 
        Reads raw QCEW data and converts it to a clean list format. 
        
        inputDir - directory containing raw data files
        fips     - the  FIPS codes to process, as string
        outfile  - the HDF output file to write to
        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyEmp' in keys: 
            outstore.remove('countyEmp')
        
        # create an empty dataframe with the right fields
        dfout = pd.DataFrame()
        
        # get the appropriate data
        pattern = inputDir + '*.q1-q4.by_area/*.q1-q4 ' + fips + '*.csv'
        infiles = glob.glob(pattern)
            
        for infile in infiles: 
            print 'Reading QCEW data in ' + infile
                
            df_allrows = pd.read_csv(infile)
            
            # first get the average earnings for all industries
            # own_code 0 is all ownership categories
            dfin = df_allrows[(df_allrows['own_code']==0) & (df_allrows['industry_title']=='Total, all industries')]
            
            year = dfin['year'][0]
            months = pd.date_range(str(year-1) + '-12-31', periods=12, freq='M') + pd.DateOffset(days=1)

            df = pd.DataFrame({'MONTH': months})
            df['AVG_MONTHLY_EARNINGS'] = np.NaN
            
            # copy the earnings data into straight file and convert weekly to monthly
            df.at[0,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==1]['avg_wkly_wage']   # jan
            df.at[3,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==2]['avg_wkly_wage']   # mar
            df.at[6,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==3]['avg_wkly_wage']   # jun
            df.at[9,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==4]['avg_wkly_wage']   # oct            
            df['AVG_MONTHLY_EARNINGS'] = df['AVG_MONTHLY_EARNINGS'] * (13.0 / 3.0)
            
            # for each industry, fill in the columns as appropriate
            industry_equiv = [
                ('TOTEMP',         '10'),                # Total, all industries
                ('RETAIL_EMP',  '44-45'),                # Retail trade
                ('EDHEALTH_EMP', '1025'),                # Education and health services
                ('LEISURE_EMP',  '1026')                 # Leisure and hospitality    
                ]                                        

            for col, industry_code in industry_equiv:                
                df[col] = 0
                
                # I need to add it up for the specific ownership titles
                # own_code indicates type of government or private sector.  >0 is all (excluding sum of them all)
                dfin = df_allrows[(df_allrows['own_code']>0) & (df_allrows['industry_code']==industry_code)]
                
                # group across ownership categories
                grouped = dfin.groupby('qtr')
                agg = grouped.agg('sum')
                
                # fill in the actual column values
                df.at[0,col] = agg.at[1,'month1_emplvl']   # jan
                df.at[1,col] = agg.at[1,'month2_emplvl']   # feb
                df.at[2,col] = agg.at[1,'month3_emplvl']   # mar
                df.at[3,col] = agg.at[2,'month1_emplvl']   # apr
                df.at[4,col] = agg.at[2,'month2_emplvl']   # may
                df.at[5,col] = agg.at[2,'month3_emplvl']   # jun
                df.at[6,col] = agg.at[3,'month1_emplvl']   # jul
                df.at[7,col] = agg.at[3,'month2_emplvl']   # aug
                df.at[8,col] = agg.at[3,'month3_emplvl']   # sep
                df.at[9,col] = agg.at[4,'month1_emplvl']   # oct
                df.at[10,col]= agg.at[4,'month2_emplvl']   # nov
                df.at[11,col]= agg.at[4,'month3_emplvl']   # dec
            
            # calculate OTHER_EMP based on the difference from the total
            df['OTHER_EMP'] = df['TOTEMP'] - df['RETAIL_EMP'] - df['EDHEALTH_EMP'] - df['LEISURE_EMP']
            
            # append to the full dataframe
            dfout = dfout.append(df, ignore_index=True)
            
        # interpolate from quarterly to monthly values
        dfout['AVG_MONTHLY_EARNINGS'] = dfout['AVG_MONTHLY_EARNINGS'].interpolate()
            
        # adjust for inflation
        dfcpi  = self.getCPIFactors(cpiFile)
        dfjoin = pd.merge(dfout, dfcpi, how='left', on=['MONTH'], sort=True)  
        dfout['AVG_MONTHLY_EARNINGS_2010USD'] = dfjoin['AVG_MONTHLY_EARNINGS'] * dfjoin['CPI_FACTOR']
                
        # write the output
        outstore.append('countyEmp', dfout, data_columns=True)        
        outstore.close()
        


        
    def processLODES(self, inputDir, lodesType, xwalkFile, fips, outfile): 
        '''
        Processes data from the LODES (LEHD Origin-Destination Employment Statistics)
        files.  Processed for SF county as a whole.
        
        inputDir - directory containing input CSV files
        lodesType - RAC, WAC or OD
                    OD file processed specifically for intra-county flows
        xwalkFile - file containing the geography crosswalk from LODES
        fips - fips code for SF county
        outfile - HDF file to write to
        '''
        
        # set characteristics for later
        fips = int(fips)
        key = 'lodes' + lodesType
        
        if lodesType=='RAC': 
            geoCol = 'h_geocode'
            wrkemp = 'WORKERS'
            filePattern = inputDir + '/RAC/ca_rac_S000_JT00_'
            
        elif lodesType=='WAC':
            geoCol = 'w_geocode'
            wrkemp = 'EMP'
            filePattern = inputDir + '/WAC/ca_wac_S000_JT00_'
            
        elif lodesType=='OD':
            hgeoCol = 'h_geocode'
            wgeoCol = 'w_geocode'
            wrkemp = 'SFWORKERS'
            filePattern = inputDir + '/OD/ca_od_main_JT00_'
            
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/' + key in keys: 
            outstore.remove(key)
            
        # read the geography crosswalk
        xwalk = pd.read_csv(xwalkFile)
        xwalk['cty'] = xwalk['cty'].astype(int)
        
        # create the output file for annual data
        years = range(self.LODES_YEARS[0], self.LODES_YEARS[1]+1)
        annual = pd.DataFrame({'YEAR': years})
        annual.index = years
        
        annual[wrkemp] = np.NaN          # total workers
        
        annual[wrkemp+'_EARN0_15'] = np.NaN  # Number of workers with earnings $1250/month or less
        annual[wrkemp+'_EARN15_40']= np.NaN  # Number of workers with earnings $1251/month to $3333/month
        annual[wrkemp+'_EARN40P']  = np.NaN  # Number of workers with earnings greater than $3333/month
        
        if lodesType=='RAC' or lodesType=='WAC': 
            annual[wrkemp+'_RETAIL']   = np.NaN  # Number of workers in retail sector
            annual[wrkemp+'_EDHEALTH'] = np.NaN  # Number of workers in education and health sector
            annual[wrkemp+'_LEISURE']  = np.NaN  # Number of workers in leisure and hospitality sector
            annual[wrkemp+'_OTHER']    = np.NaN  # Number of workers in other sectors
        
        
        # get the data for each year
        for year in years: 
            
            # read the data and aggregate to county level
            infile = filePattern + str(year) + '.csv' 
            if os.path.isfile(infile):
                    
                print 'Reading LODES data in ' + infile            
                df = pd.read_csv(infile)            
                
                # one dimensional processing for RAC and WAC
                if lodesType=='RAC' or lodesType=='WAC': 
                    df = pd.merge(df, xwalk, how='left', left_on=geoCol, right_on='tabblk2010')            
                    df = df[df['cty']==fips]            
                    agg = df.groupby('cty').agg('sum')
                    
                    # copy over the appropriate fields
                    annual.at[year, wrkemp] = agg.at[fips, 'C000']        
                    
                    annual.at[year, wrkemp+'_EARN0_15'] = agg.at[fips, 'CE01']
                    annual.at[year, wrkemp+'_EARN15_40']= agg.at[fips, 'CE02'] 
                    annual.at[year, wrkemp+'_EARN40P']  = agg.at[fips, 'CE03'] 
                    
                    annual.at[year, wrkemp+'_RETAIL']   = agg.at[fips, 'CNS07'] 
                    annual.at[year, wrkemp+'_EDHEALTH'] = agg.at[fips, 'CNS15'] + agg.at[fips, 'CNS16'] 
                    annual.at[year, wrkemp+'_LEISURE']  = agg.at[fips, 'CNS17'] + agg.at[fips, 'CNS18'] 
                    annual.at[year, wrkemp+'_OTHER']    = (annual.at[year, wrkemp] 
                                                        -annual.at[year, wrkemp+'_RETAIL']
                                                        -annual.at[year, wrkemp+'_EDHEALTH']
                                                        -annual.at[year, wrkemp+'_LEISURE']
                                                        )
                
                # for OD, keep only intra-county flows
                elif lodesType=='OD': 
                    df = pd.merge(df, xwalk, how='left', left_on=hgeoCol, right_on='tabblk2010')   
                    df = pd.merge(df, xwalk, how='left', left_on=wgeoCol, right_on='tabblk2010', suffixes=('_h', '_w'))           
                    df = df[(df['cty_h']==fips) & (df['cty_w']==fips)]     
                           
                    agg = df.groupby('cty_h').agg('sum')
                    
                    # copy over the appropriate fields
                    annual.at[year, wrkemp] = agg.at[fips, 'S000']        
                    
                    annual.at[year, wrkemp+'_EARN0_15'] = agg.at[fips, 'SE01']
                    annual.at[year, wrkemp+'_EARN15_40']= agg.at[fips, 'SE02'] 
                    annual.at[year, wrkemp+'_EARN40P']  = agg.at[fips, 'SE03'] 
        
        # convert data to monthly
        monthly = self.convertAnnualToMonthly(annual)
        
        # append to the output store
        outstore.append(key, monthly, data_columns=True)
        outstore.close()
        

    def processFuelPriceData(self, fuelFile, cpiFile, outfile): 
        """ 
        Reads raw QCEW data and converts it to a clean list format. 
        
        fuelFile - file containing data from EIA
        outfile  - the HDF output file to write to
        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/fuelPrice' in keys: 
            outstore.remove('fuelPrice')
        
        # get raw data
        df = pd.read_excel(fuelFile, sheetname='Data 4', skiprows=2)
        df = df.rename(columns={
                       'Date': 'MONTH', 
                       'San Francisco All Grades All Formulations Retail Gasoline Prices (Dollars per Gallon)': 'FUEL_PRICE'
                       })
        df = df[['MONTH', 'FUEL_PRICE']]
        
        # normalize to the first day of the month
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=-14))
                
        # adjust the fuel price for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        df['FUEL_PRICE_2010USD'] = df['FUEL_PRICE'] * df['CPI_FACTOR']
        
        # keep only the relevant columns
        df = df[['MONTH', 'FUEL_PRICE', 'FUEL_PRICE_2010USD', 'CPI']]        
        
        # append to the output store
        outstore.append('fuelPrice', df, data_columns=True)
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


    def convertAnnualToMonthly(self, annual): 
        '''
        Convert annual dataframe to monthly dataframe. 
        Use linear interpolation to interpolate values, and extend to end
        of year.  
        
        '''        

        # extrapolate the final year to get the last 6 months of data
        extraYear = annual['YEAR'].max() + 1
        annual.loc[extraYear] = np.NaN
        annual.at[extraYear, 'YEAR'] = extraYear
        for col in annual.columns:
            annual.at[extraYear, col] =(annual.at[extraYear-1, col] + 
                                       (annual.at[extraYear-1, col] 
                                       -annual.at[extraYear-2, col]))
        
        # expand to monthly, and interpolate values
        annual['MONTH'] = annual['YEAR'].apply(lambda x: pd.Timestamp(str(int(x)) + '-07-01'))
        annual = annual.set_index(pd.DatetimeIndex(annual['MONTH']))
        
        monthly = annual[['MONTH']].resample('M')
        monthly['MONTH'] = monthly.index
        monthly['MONTH'] = monthly['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
                
        monthly = pd.merge(monthly, annual, how='left', on=['MONTH'], sort=True)  
        monthly = monthly.set_index(pd.DatetimeIndex(monthly['MONTH']))   
        
        monthly = monthly.interpolate()
        
        # drop the extraYear, which was just used to get to the end of the last year
        monthly = monthly[monthly['YEAR']<extraYear-0.5]
        monthly = monthly.drop('YEAR', 1)
        
        # convert to integers
        for col in monthly.columns:  
            if monthly[col].dtype == float: 
                try: 
                    monthly[col] = monthly[col].astype(int)
                except ValueError: 
                    print 'Cannot convert NA values to int for column ', col
        
        # set a unique index
        monthly.index = pd.Series(range(0,len(monthly)))
                
        return monthly

    