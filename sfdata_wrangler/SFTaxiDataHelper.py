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
import datetime

                    
                                    
class SFTaxiDataHelper():
    """ 
    Methods used to read taxi GPS points and use them to calculate 
    link speeds. 
    """

    # number of rows to read at a time
    CHUNKSIZE = 100000

    def __init__(self):
        """
        Constructor.             
        """   
                    
    def processRawData(self, infile, outfile):
        """
        Read taxi data, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in raw CSV format
        outfile - output file name in h5 format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # set up the reader
        reader = pd.read_csv(infile,  
                         sep = '\t',
                         iterator = True, 
                         chunksize= self.CHUNKSIZE)

        # establish the writer
        store = pd.HDFStore(outfile)

        # iterate through chunk by chunk so we don't run out of memory
        rowsRead    = 0
        rowsWritten = 0
        for chunk in reader:   

            rowsRead    += len(chunk)
        
            # convert to timedate formats
            chunk['time'] = pd.to_datetime(chunk['time'],format="%Y-%m-%d %H:%M:%S")        

            # write the data
            try: 
                store.append('points', chunk, data_columns=True)
            except ValueError: 
                store = pd.HDFStore(outfile)
                print 'Structure of HDF5 file is: '
                print store.points.dtypes
                store.close()
                
                print 'Structure of current dataframe is: '
                print chunk.dtypes
                
                raise  
            except TypeError: 
                print 'Structure of current dataframe is: '
                types = chunk.dtypes
                for type in types:
                    print type
                
                raise

            rowsWritten += len(chunk)
            print 'Read %i rows and kept %i rows.' % (rowsRead, rowsWritten)
            
        # close the writer
        store.close()
    
    
    
        