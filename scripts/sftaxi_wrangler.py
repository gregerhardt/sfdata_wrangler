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
from SFTaxiDataHelper import SFTaxiDataHelper

USAGE = r"""

 python sftaxi_wrangler.py [stepsToRun]
   
 e.g.

 python clean matchPoints pathIdent timeAlloc timeAgg
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'readNetwork', 
                'matchPoints', 
                'pathID', 
                'timeAlloc', 
                'timeAgg'
                ]    
                

# INPUT FILES--change as needed
HWYNET_FILE = "C:/CASA/Data/network/transcad/SanFranciscoSubArea_2010-assign.shp"

RAW_TAXI_FILES =["C:/CASA/Data/taxi/2009-02-13.txt"
                ]
    

# OUTPUT FILES--change as needed
TAXI_OUTFILE   = "C:/CASA/DataExploration/taxi.h5"    
HWYNET_OUTFILE = "C:/CASA/DataExploration/hwynet.h5"    


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
    sftaxiHelper = SFTaxiDataHelper()
    
    # reads the network into a networkX format
    #    OUTPUT: network data structure
    if 'readNetwork' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        netHelper = NetworkHelper()
        netHelper.readShapeFile(HWYNET_FILE) 
        print 'Finished reading highway network in ', (datetime.datetime.now() - startTime)


    # convert the raw data and join closest node to each taxi GPS point. 
    #    OUTPUT: list of points with network node IDs appended
    if 'matchPoints' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        for infile in RAW_TAXI_FILES: 
            sftaxiHelper.processRawData(infile, TAXI_OUTFILE)
        print 'Finished matching taxi gps points to network nodes in ', (datetime.datetime.now() - startTime)

    # identify the paths traversed in the network
    #   OUTPUT: list of path objects
    if 'pathID' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
#        netHelper = NetworkHelper(HWYNET_FILE)
#        sftaxiHelper.identifyPaths(TAXI_OUTFILE, netHelper)  
        print 'Finished identifying paths in ', (datetime.datetime.now() - startTime) 
    
    # allocate the travel times from paths to links
    #   OUTPUT: list of link objects, with duplicates
    if 'timeAlloc' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        
        print 'Finished allocating travel time to links in ', (datetime.datetime.now() - startTime) 

    # aggregate the link travel times
    #   OUTPUT: list of link objects, without duplicates
    if 'timeAgg' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        
        print 'Finished aggregating link travel times in ', (datetime.datetime.now() - startTime) 
        
    print 'Run complete!  Time for a pint!'
    
    