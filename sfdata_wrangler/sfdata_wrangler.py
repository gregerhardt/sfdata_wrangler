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
from GTFSHelper import GTFSHelper


def processSFMuniData(outfile, aggfile, routeEquivFile):
    """
    Reads text files containing SFMuni AVL/APC data and converts them to a 
    processed and aggregated HDF file.           
    
    outfile - HDF file containing processed disaggregate data
    aggfile - HDF file containing processed aggregate data   
    routeEquivFile - CSV file containing equivalency between AVL route IDs
                     and GTFS route IDs.                  
    """

    
    startTime = datetime.datetime.now()   
    print 'Started processing SFMuni data at ', startTime
    sfmuniHelper = SFMuniDataHelper()
    sfmuniHelper.readRouteEquiv(routeEquivFile)

    # convert the data
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0803.stp", outfile)
    sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0906.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0912.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1001.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1005.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1009.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1101.stp", outfile)
    #sfmuniHelper.processRawData("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1110.stp", outfile)    
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
    print 'Finished converting SFMuni data in ', (convertedTime - startTime)


def processGTFS(outfile):
    """
    Reads files containing SFMuni General Transit Feed Specification, and converts
    them to schedule format for joining to AVL/APC data.           
    
    outfile - HDF file containing processed GTFS data             
    """

    startTime = datetime.datetime.now()   
    print 'Started processing GTFS at ', startTime
    gtfsHelper = GTFSHelper()

    # convert the data
    gtfsHelper.processRawData("C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20091106_0310.zip", outfile)
        
    convertedTime = datetime.datetime.now() 
    print 'Finished converting GTFS in ', (convertedTime - startTime)
    
    
def joinGTFSandSFMuniData(gtfs_file, sfmuni_file, joined_outfile):
    """
    Left join from GTFS to SFMuni sample.        
    
    gtfs_file - HDF file containing processed GTFS data      
    sfmuni_file - HDF file containing processed, just for sampled routes
    joined_outfile - HDF file containing merged GTFS and SFMuni data     
    """

    startTime = datetime.datetime.now()   
    print 'Started joining GTFS and SFMuni data at ', startTime
    gtfsHelper = GTFSHelper()

    # join the data
    gtfsHelper.joinSFMuniData(gtfs_file, sfmuni_file, joined_outfile)
        
    convertedTime = datetime.datetime.now() 
    print 'Finished joining GTFS and SFMuni data in ', (convertedTime - startTime)
    

if __name__ == "__main__":
    
    # eventually convert filenames to arguments
    route_equiv = "C:/CASA/Data/MUNI/routeEquiv.csv"
    
    sfmuni_outfile = "C:/CASA/DataExploration/sfmuni.h5"
    
    gtfs_outfile = "C:/CASA/DataExploration/gtfs.h5"
    
    joined_outfile = "C:/CASA/DataExploration/transit_expanded.h5"
    
    sfmuni_aggfile = "C:/CASA/DataExploration/sfmuni_aggregate.h5"

    imputed_outfile = "C:/CASA/DataExploration/sfmuni_imputed.h5"
    

    #processSFMuniData(sfmuni_outfile, sfmuni_aggfile, route_equiv)
    #processGTFS(gtfs_outfile)
    #joinGTFSandSFMuniData(gtfs_outfile, sfmuni_outfile, joined_outfile)

    startTime = datetime.datetime.now()   
    print 'Started aggregating data at ', startTime
    
    # create the helper object
    sfmuniHelper = SFMuniDataHelper()
    
    # calculate monthly averages, and aggregate the unweighted data
    #sfmuniHelper.calcMonthlyAverages(joined_outfile, sfmuni_aggfile, 'expanded', 'df')
    #sfmuniHelper.calculateRouteStopTotals(sfmuni_aggfile, 'df',  'route_stops')
    #sfmuniHelper.calculateRouteTotals(sfmuni_aggfile, 'route_stops',  'routes')  
    #sfmuniHelper.calculateStopTotals(sfmuni_aggfile, 'route_stops',  'stops')
    #sfmuniHelper.calculateSystemTotals(sfmuni_aggfile, 'route_stops',  'system')
    
    # impute the data, and add weights.  Calculate new aggregations. 
    #sfmuniHelper.imputeMissingValuesByMonth(sfmuni_aggfile, imputed_outfile, 'df', 'df')
    sfmuniHelper.calculateRouteStopTotals(imputed_outfile, 'df',  'route_stops')
    sfmuniHelper.calculateRouteTotals(imputed_outfile, 'route_stops',  'routes')  
    sfmuniHelper.calculateStopTotals(imputed_outfile, 'route_stops',  'stops')
    sfmuniHelper.calculateSystemTotals(imputed_outfile, 'route_stops',  'system')
        
    aggregatedTime = datetime.datetime.now()
    print 'Finished aggregating SFMuni data in ', (aggregatedTime - startTime) 
    