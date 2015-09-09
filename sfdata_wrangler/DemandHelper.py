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
import glob

    
class DemandHelper():
    """ 
    Class to create drivers of demand data: employment, population, fuel cost.
    
    """

    # the most recent year included in the census population estimates
    MAX_YEAR = 2014

    def __init__(self):
        '''
        Constructor. 

        '''   
    
    def processQCEWData(self, inputDir, fips, outfile): 
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
        
        # get the appropriate data
        pattern = inputDir + '*.q1-q4.by_area/*.q1-q4 ' + fips + '*.csv'
        infiles = glob.glob(pattern)
            
        monthCount = 0
        for infile in infiles: 
            print 'Reading QCEW data in ' + infile
                
            dfin = pd.read_csv(infile)
            dfin = dfin[(dfin['own_title']=='Total Covered') & (dfin['industry_title']=='Total, all industries')]
            
            year = dfin['year'][0]
            months = pd.date_range(str(year-1) + '-12-31', periods=12, freq='M') + pd.DateOffset(days=1)
                
            dfout = pd.DataFrame({'MONTH': months})
            dfout['TOTEMP'] = 0

            # copy the data into straight file
            dfout.at[0,'TOTEMP'] = dfin[dfin['qtr']==1]['month1_emplvl']   # jan
            dfout.at[1,'TOTEMP'] = dfin[dfin['qtr']==1]['month2_emplvl']   # feb
            dfout.at[2,'TOTEMP'] = dfin[dfin['qtr']==1]['month3_emplvl']   # mar
            dfout.at[3,'TOTEMP'] = dfin[dfin['qtr']==2]['month1_emplvl']   # apr
            dfout.at[4,'TOTEMP'] = dfin[dfin['qtr']==2]['month2_emplvl']   # may
            dfout.at[5,'TOTEMP'] = dfin[dfin['qtr']==2]['month3_emplvl']   # jun
            dfout.at[6,'TOTEMP'] = dfin[dfin['qtr']==3]['month1_emplvl']   # jul
            dfout.at[7,'TOTEMP'] = dfin[dfin['qtr']==3]['month2_emplvl']   # aug
            dfout.at[8,'TOTEMP'] = dfin[dfin['qtr']==3]['month3_emplvl']   # sep
            dfout.at[9,'TOTEMP'] = dfin[dfin['qtr']==4]['month1_emplvl']   # oct
            dfout.at[10,'TOTEMP']= dfin[dfin['qtr']==4]['month2_emplvl']   # nov
            dfout.at[11,'TOTEMP']= dfin[dfin['qtr']==4]['month3_emplvl']   # dec
                
            # set a unique index
            dfout.index = monthCount + pd.Series(range(0,len(dfout)))
            monthCount += len(dfout)
                
            # append to the output store
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
        annual = pd.DataFrame({'YEAR': range(2000, self.MAX_YEAR+1)})
        annual['POP'] = 0
        annual.index = range(2000, self.MAX_YEAR+1)
        
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
        
        for year in range(2010, self.MAX_YEAR+1): 
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
        
        dfcpi['FACTOR'] = base / dfcpi['CPI']
        
        # adjust the fuel price for inflation
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        df['FUEL_PRICE_2010USD'] = df['FUEL_PRICE'] * df['FACTOR']
        
        # keep only the relevant columns
        df = df[['MONTH', 'FUEL_PRICE', 'FUEL_PRICE_2010USD', 'CPI']]        
        
        # append to the output store
        outstore.append('fuelPrice', df, data_columns=True)
        outstore.close()
        
        