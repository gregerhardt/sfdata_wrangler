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

from SFMuniDataHelper import SFMuniDataHelper
from DataFrameViewer import DataFrameViewer



if __name__ == "__main__":
    
    # eventually convert filenames to arguments
    outfile = "C:/CASA/DataExploration/sfmuni.h5"
    
    startTime = datetime.datetime.now()   
    print 'Started at ', startTime
    sfmuniHelper = SFMuniDataHelper()

    # convert the data
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1201.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1203.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1206.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1209.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1212.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1303.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1304.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1306.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1308.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1310.stp", outfile)
        
    convertedTime = datetime.datetime.now() 
    print 'Finished converting data in ', (convertedTime - startTime)
    
    # calculate monthly averages
    sfmuniHelper.calcMonthlyAverages(outfile, 'weekday2')
    sfmuniHelper.calcMonthlyAverages(outfile, 'saturday')
    sfmuniHelper.calcMonthlyAverages(outfile, 'sunday')

    
    # aggregate different dimensions
    #sfmuniHelper.aggregateStops(outfile, 'daily_trips', 
    #    ['ROUTE', 'PATTCODE', 'DIR', 'TRIP'])
    #    
    #sfmuniHelper.aggregateTrips(outfile, 'daily_route_stops', 
    #    ['ROUTE', 'PATTCODE', 'DIR', 'SEQ'])
    #    
    #sfmuniHelper.aggregateStopsAndTrips(outfile, 'daily', ['ROUTE'])
    #    
    #aggregatedTime = datetime.datetime.now()
    #print 'Finished aggregating data in ', (aggregatedTime - convertedTime) 
                
    # read it back in
    #store = pd.HDFStore(outfile)
    #df = store.df[500:1500]    

    # let the user view the first 1000 rows
    #vw = DataFrameViewer()
    #vw.view(df)
