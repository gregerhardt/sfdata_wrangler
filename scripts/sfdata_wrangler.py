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

import sys
import datetime

sys.path.append('C:/CASA/Workspace/sfdata_wrangler/sfdata_wrangler')
from SFMuniDataHelper import SFMuniDataHelper
from GTFSHelper import GTFSHelper


USAGE = r"""

 python sfdata_wrangler.py [stepsToRun]
   
 e.g.

 python sfdata_wrangler convertAVLAPC expand aggUnweighted aggWeighted
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'convertAVLAPC', 
                'expand', 
                'aggUnweighted', 
                'aggWeighted'
                ]    
                

# INPUT FILES--change as needed
ROUTE_EQUIV = "C:/CASA/Data/MUNI/routeEquiv.csv"

RAW_STP_FILES =["C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0803.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0906.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/0912.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1001.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1005.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1009.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1101.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1110.stp",    
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1201.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1203.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1206.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1209.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1212.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1303.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1304.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1306.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1308.stp",
                "C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1310.stp"
                ]
    
# these should be ordered from old to new, and the software will fill in any gaps
RAW_GTFS_FILES = [
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20090402_0310.zip",  # 20090221 to 20090626
                                                                                           # overlap of 13 days
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20091106_0310.zip",  # 20090613 to 20091204
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100415_0222.zip",  # 20091205 to 20100507
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100812_0223.zip",  # 20100508 to 20100903
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100908_0243.zip",  # 20100904 to 20101231
                                                                                           # gap of 2 days
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110104_0839.zip",  # 20110103 to 20110121
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110420_0243.zip",  # 20110122 to 20110610
                                                                                           # gap of 2 days
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110910_0449.zip",  # 20110613 to 20111014
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20111210_0410.zip",  # 20111015 to 20120120
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20120319_0337.zip",  # 20120121 to 20120615
                                                                                           # overlap of 6 days
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20120908_0325.zip",  # 20120609 to 20120928
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130302_0432.zip",  # 20120929 to 20130322
                                                                                           # gap of 8 days
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130612_0307.zip",  # 20130330 to 20130628
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130910_2349.zip",  # 20130629 to 20131025
  "C:/CASA/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20140117_0111.zip"   # 20131026 to 20140131  
  ]
        

# OUTPUT FILES--change as needed
SFMUNI_OUTFILE  = "C:/CASA/DataExploration/sfmuni.h5"    
EXPANDED_OUTFILE= "C:/CASA/DataExploration/transit_expanded.h5"    
SFMUNI_AGGFILE  = "C:/CASA/DataExploration/sfmuni_aggregate.h5"
IMPUTED_AGGFILE = "C:/CASA/DataExploration/sfmuni_imputed.h5"


# main function call
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print USAGE
        print 'Valid steps include: ', VALID_STEPS
        sys.exit(2)

    STEPS_TO_RUN = sys.argv[1:]
    for step in STEPS_TO_RUN: 
        if not (step in VALID_STEPS): 
            print step, ' is not a valid step to run'
            print 'Valid steps include: ', VALID_STEPS
            sys.exit(2)
    
    # create the helper
    sfmuniHelper = SFMuniDataHelper()
    sfmuniHelper.readRouteEquiv(ROUTE_EQUIV)

    # convert the AVL/APC data
    if 'convertAVLAPC' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        for infile in RAW_STP_FILES: 
            sfmuniHelper.processRawData(infile, SFMUNI_OUTFILE)
        print 'Finished converting SFMuni data in ', (datetime.datetime.now() - startTime)

    # process GTFS data, and join AVL/APC data to it. 
    if 'expand' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        gtfsHelper = GTFSHelper()
        for infile in RAW_GTFS_FILES: 
            gtfsHelper.processRawData(infile, SFMUNI_OUTFILE, EXPANDED_OUTFILE)        
        print 'Finished converting and joining GTFS in ', (datetime.datetime.now() - startTime)

    # calculate monthly averages, and aggregate the unweighted data
    if 'aggUnweighted' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        sfmuniHelper.calcMonthlyAverages(EXPANDED_OUTFILE, SFMUNI_AGGFILE, 'expanded', 'df')
        sfmuniHelper.calculateRouteStopTotals(SFMUNI_AGGFILE, 'df',  'route_stops')
        sfmuniHelper.calculateRouteTotals(SFMUNI_AGGFILE, 'route_stops',  'routes')  
        sfmuniHelper.calculateStopTotals(SFMUNI_AGGFILE, 'route_stops',  'stops')
        sfmuniHelper.calculateSystemTotals(SFMUNI_AGGFILE, 'route_stops',  'system')
        print 'Finished unweighted aggregations in ', (datetime.datetime.now() - startTime) 
    
    # impute the data, and add weights.  Calculate new aggregations. 
    if 'aggWeighted' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        sfmuniHelper.imputeMissingValuesByMonth(SFMUNI_AGGFILE, IMPUTED_AGGFILE, 'df', 'df')
        sfmuniHelper.calculateRouteStopTotals(IMPUTED_AGGFILE, 'df',  'route_stops')
        sfmuniHelper.calculateRouteTotals(IMPUTED_AGGFILE, 'route_stops',  'routes')  
        sfmuniHelper.calculateStopTotals(IMPUTED_AGGFILE, 'route_stops',  'stops')
        sfmuniHelper.calculateSystemTotals(IMPUTED_AGGFILE, 'route_stops',  'system')
        print 'Finished weighted aggregations in ', (datetime.datetime.now() - startTime) 
        
    print 'Run complete!'
    
    