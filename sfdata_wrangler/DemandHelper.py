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

    
class DemandHelper():
    """ 
    Class to create drivers of demand data: employment, population, fuel cost.
    
    """

    # the range of years for these data files
    POP_EST_YEARS = [2000,2014]
    LODES_YEARS   = [2002,2013]
    

    def __init__(self):
        '''
        Constructor. 

        '''   
    
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
        
        return dfcpi

    
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
        dfout['AVG_MONTHLY_EARNINGS_USD2010'] = dfjoin['AVG_MONTHLY_EARNINGS'] * dfjoin['CPI_FACTOR']
        
        dfout.to_csv('c:/temp/qcew2.csv')
        
        # write the output
        outstore.append('countyEmp', dfout, data_columns=True)        
        outstore.close()
        

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

        
        # expand to monthly, and interpolate values
        annual['MONTH'] = annual['YEAR'].apply(lambda x: pd.Timestamp(str(x) + '-07-01'))
        annual = annual.set_index(pd.DatetimeIndex(annual['MONTH']))
        
        monthly = annual[['MONTH']].resample('M')
        monthly['MONTH'] = monthly.index
        monthly['MONTH'] = monthly['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
                
        monthly = pd.merge(monthly, annual, how='left', on=['MONTH'], sort=True)  
        monthly = monthly[['MONTH', 'POP']]
        monthly = monthly.set_index(pd.DatetimeIndex(monthly['MONTH']))     
        
        monthly = monthly.interpolate()
        
        # set a unique index
        monthly.index = pd.Series(range(0,len(monthly)))
                
        # append to the output store
        outstore.append('countyPop', monthly, data_columns=True)
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
        
        
    def processLODESRAC(self, inputDir, xwalkFile, fips, outfile): 
        '''
        Processes data from the LODES (LEHD Origin-Destination Employment Statistics)
        RAC (Resident Area Characteristics) files.  This gives us information
        on workers at their home location.  Processed for SF county as a whole.
        
        inputDir - directory containing input CSV files
        xwalkFile - file containing the geography crosswalk from LODES
        fips - fips code for SF county
        outfile - HDF file to write to
        '''
        
        fips = int(fips)
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/lodesRAC' in keys: 
            outstore.remove('lodesRAC')
            
        # read the geography crosswalk
        xwalk = pd.read_csv(xwalkFile)
        xwalk['cty'] = xwalk['cty'].astype(int)
        
        # create the output file for annual data
        years = range(self.LODES_YEARS[0], self.LODES_YEARS[1]+2)
        annual = pd.DataFrame({'YEAR': years})
        annual.index = years
        
        annual['WORKERS'] = np.NaN          # total workers
        
        annual['WORKERS_LOWINC'] = np.NaN  # Number of workers with earnings $1250/month or less
        annual['WORKERS_MIDINC'] = np.NaN  # Number of workers with earnings $1251/month to $3333/month
        annual['WORKERS_HIGHINC']= np.NaN  # Number of workers with earnings greater than $3333/month
        
        annual['WORKERS_RETAIL']   = np.NaN  # Number of workers in retail sector
        annual['WORKERS_EDHEALTH'] = np.NaN  # Number of workers in education and health sector
        annual['WORKERS_LEISURE']  = np.NaN  # Number of workers in leisure and hospitality sector
        annual['WORKERS_OTHER']    = np.NaN  # Number of workers in other sectors
        
        
        # get the data for each year
        for year in years: 
            
            # read the data and aggregate to county level
            infile = inputDir + '/ca_rac_S000_JT00_' + str(year) + '.csv' 
            if os.path.isfile(infile):
                    
                print 'Reading LODES RAC data in ' + infile            
                df = pd.read_csv(infile)            
                df = pd.merge(df, xwalk, how='left', left_on='h_geocode', right_on='tabblk2010')            
                df = df[df['cty']==fips]            
                agg = df.groupby('cty').agg('sum')
                
                # copy over the appropriate fields
                annual.at[year, 'WORKERS'] = agg.at[fips, 'C000']        
                
                annual.at[year, 'WORKERS_LOWINC'] = agg.at[fips, 'CE01']
                annual.at[year, 'WORKERS_MIDINC'] = agg.at[fips, 'CE02'] 
                annual.at[year, 'WORKERS_HIGHINC']= agg.at[fips, 'CE03'] 
                
                annual.at[year, 'WORKERS_RETAIL']   = agg.at[fips, 'CNS07'] 
                annual.at[year, 'WORKERS_EDHEALTH'] = agg.at[fips, 'CNS15'] + agg.at[fips, 'CNS16'] 
                annual.at[year, 'WORKERS_LEISURE']  = agg.at[fips, 'CNS17'] + agg.at[fips, 'CNS18'] 
                annual.at[year, 'WORKERS_OTHER']    = (annual.at[year, 'WORKERS'] 
                                                    -annual.at[year, 'WORKERS_RETAIL']
                                                    -annual.at[year, 'WORKERS_EDHEALTH']
                                                    -annual.at[year, 'WORKERS_LEISURE']
                                                    )
                                                    
        # extrapolate the final year to get the last 6 months of data
        extraYear = self.LODES_YEARS[1] + 1
        for col in annual.columns:
            annual.at[extraYear, col] =(annual.at[extraYear-1, col] + 
                                       (annual.at[extraYear-1, col] 
                                       -annual.at[extraYear-2, col]))
                        
        # expand to monthly, and interpolate values
        annual['MONTH'] = annual['YEAR'].apply(lambda x: pd.Timestamp(str(x) + '-07-01'))
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
                monthly[col] = monthly[col].astype(int)
        
        # set a unique index
        monthly.index = pd.Series(range(0,len(monthly)))
        
        monthly.to_csv('c:/temp/rac.csv')
        
        # append to the output store
        outstore.append('lodesRAC', monthly, data_columns=True)
        outstore.close()
        