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
import datetime
import glob 


class BARTHelper():
    """ 
    Methods used to read BART entry/exit data into pandas dataframe. 

    """
    
    MONTHS = ['January', 
              'February', 
              'March', 
              'April', 
              'May', 
              'June', 
              'July', 
              'August', 
              'September', 
              'October', 
              'November', 
              'December']
    
    
    def __init__(self):
        """
        Constructor.                 
        """        
        

    def processFiles(self, indir, outfile, outkey):
        """
        Read data, cleans it, processes it, and writes it to an HDF5 file.
        
        indir - the year will be appended here        
        outfile - output file name in h5 format
        outkey - write to this key
        """
        
        # initialize a few things
        numRecords = 0
        outstore = pd.HDFStore(outfile) 
        if outkey in outstore.keys(): 
            outstore.remove(outkey)        
        
        # loop through each directly and get the right files
        dirs = glob.glob(indir + '*/')
        for d in dirs: 
            year = d[-5:-1]
            print 'Processing files in ' + d            
            
            for month in self.MONTHS: 
                files = glob.glob(d + '/*' + month + '*')
                
                if len(files)==1: 
                                        
                    # first get the number of stations
                    df_wholesheet = pd.read_excel(files[0], sheetname=0, header=1, index_col=0)
                    num_stations = df_wholesheet.columns.tolist().index('Exits')
                    footer_rows = len(df_wholesheet) - num_stations - 1
                    
                    # now get actual data and convert formats
                    df = pd.read_excel(files[0], sheetname=0, header=1, 
                                skip_footer=footer_rows, index_col=0, parse_cols=num_stations+1)
                    df = pd.DataFrame(df.stack())
                    
                    df = df.reset_index()
                    df = df.rename(columns={'level_0' : 'FROM' , 'level_1' : 'TO', 0 : 'RIDERS'})
                    
                    # make sure numbers are stored as strings
                    df['FROM'] = df['FROM'].apply(str)
                    df['TO'] = df['TO'].apply(str)
    
                    # set a few extra fields
                    df['MONTH'] = pd.to_datetime(month + ' 1, ' + year)
                    df['STATIONS'] = num_stations
                    
                    # give it a unique index
                    df.index = pd.Series(range(numRecords,numRecords+len(df))) 
                    numRecords += len(df)
                    
                    # append the data
                    outstore.append(outkey, df, data_columns=True)


                
                
                