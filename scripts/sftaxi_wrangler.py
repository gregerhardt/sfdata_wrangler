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
import pandas as pd

sys.path.append('C:/CASA/Workspace/dta')
sys.path.append('C:/CASA/Workspace/Path-Inference-Filter/mm')
sys.path.append('C:/CASA/Workspace/sfdata_wrangler/sfdata_wrangler')

import dta
from SFTaxiDataHelper import SFTaxiDataHelper
from NetworkHelper import NetworkHelper

USAGE = r"""

 python sftaxi_wrangler.py [stepsToRun]
   
 e.g.

 python clean matchPoints pathIdent timeAlloc timeAgg
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'readNetwork',                 
                'convertPoints', 
                'extractTrips', 
                'pathID', 
                'timeAlloc', 
                'timeAgg'
                ]    
                

# INPUT FILES--change as needed
INPUT_DYNAMEQ_NET_DIR    = "C:/CASA/Data/network/dynameq/validation2010.july19_Sig/Reports/Export"
INPUT_DYNAMEQ_NET_PREFIX = "pb_july19_830p"

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
    
    # reads the SF network, using from the Dynameq structure. 
    if 'readNetwork' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        netHelper = NetworkHelper()
        net = netHelper.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX) 
        print 'Finished reading highway network in ', (datetime.datetime.now() - startTime)

    # convert the taxi data
    if 'convertPoints' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        for infile in RAW_TAXI_FILES: 
            sftaxiHelper.processRawData(infile, TAXI_OUTFILE)
        print 'Finished converting taxi GPS data in ', (datetime.datetime.now() - startTime)

    # extract trips
    if 'extractTrips' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        sftaxiHelper.extractGPSTrips(TAXI_OUTFILE)            
        print 'Finished extracting taxi trips in ', (datetime.datetime.now() - startTime)

    

    """
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
    """
    
            
    print 'Run complete!  Time for a pint!'
    
    