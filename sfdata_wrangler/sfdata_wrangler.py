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

import datetime

from SFMuniDataHelper import SFMuniDataHelper



if __name__ == "__main__":
    
    # eventually convert filenames to arguments
    outfile = "C:/CASA/DataExploration/sfmuni.h5"
    
    startTime = datetime.datetime.now()   
    print 'Started at ', startTime
    sfmuniHelper = SFMuniDataHelper()

    # convert the data
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0803.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0906.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0912.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1001.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1005.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1009.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1101.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1110.stp", outfile)    
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1201.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1203.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1206.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1209.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1212.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1303.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1304.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1306.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1308.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1310.stp", outfile)
        
    convertedTime = datetime.datetime.now() 
    print 'Finished converting data in ', (convertedTime - startTime)
    
    # calculate monthly averages
    sfmuniHelper.calcMonthlyAverages(outfile, 'weekday')
    sfmuniHelper.calcMonthlyAverages(outfile, 'saturday')
    sfmuniHelper.calcMonthlyAverages(outfile, 'sunday')

    # aggregate trips into daily totals        
    sfmuniHelper.aggregateTrips(outfile, 'weekday',  'weekday_route_stops')
    sfmuniHelper.aggregateTrips(outfile, 'saturday', 'saturday_route_stops')
    sfmuniHelper.aggregateTrips(outfile, 'sunday',   'sunday_route_stops')

    # sum route totals
    sfmuniHelper.calculateRouteTotals(outfile, 'weekday_route_stops',  'weekday_routes')
    sfmuniHelper.calculateRouteTotals(outfile, 'saturday_route_stops', 'saturday_routes')
    sfmuniHelper.calculateRouteTotals(outfile, 'sunday_route_stops',   'sunday_routes')    
    
    # sum stop totals    
    sfmuniHelper.calculateStopTotals(outfile, 'weekday_route_stops',  'weekday_stops')
    sfmuniHelper.calculateStopTotals(outfile, 'saturday_route_stops', 'saturday_stops')
    sfmuniHelper.calculateStopTotals(outfile, 'sunday_route_stops',   'sunday_stops')    
    
        
    aggregatedTime = datetime.datetime.now()
    print 'Finished aggregating data in ', (aggregatedTime - convertedTime) 
                
