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

sys.path.append('C:/CASA/Workspace/dta')
sys.path.append('C:/CASA/Workspace/Path-Inference-Filter')
sys.path.append('C:/CASA/Workspace/sfdata_wrangler/sfdata_wrangler')

from TaxiDataHelper import TaxiDataHelper
from HwyNetwork import HwyNetwork

USAGE = r"""

 python sftaxi_wrangler.py [stepsToRun]
   
 e.g.

 python readNetwork convertPoints identifyTrips createTraj timeAlloc TimeAgg
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'convertPoints', 
                'identifyTrips', 
                'createTraj', 
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
    taxiHelper = TaxiDataHelper()
    
    # convert the taxi data
    if 'convertPoints' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        for infile in RAW_TAXI_FILES: 
            taxiHelper.processRawData(infile, TAXI_OUTFILE, 'points')
        print 'Finished converting taxi GPS data in ', (datetime.datetime.now() - startTime)

    # extract trips
    if 'identifyTrips' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        taxiHelper.identifyGPSTrips(TAXI_OUTFILE, 'points', 'trip_points')            
        print 'Finished identifying taxi trips in ', (datetime.datetime.now() - startTime)

    # create trajectories
    if 'createTraj' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        hwynet = HwyNetwork()
        hwynet.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX) 
        hwynet.initializeShortestPaths()
        print 'Finished preparing highway network in ', (datetime.datetime.now() - startTime)
        
        startTime = datetime.datetime.now()   
        taxiHelper.createTrajectories(hwynet, TAXI_OUTFILE, 'trip_points', 'trajectories') 
        print 'Finished creating taxi trajectories in ', (datetime.datetime.now() - startTime)

            
    print 'Run complete!  Time for a pint!'
    
    