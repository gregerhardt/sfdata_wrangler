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
from Visualizer import Visualizer

USAGE = r"""

 python sftaxi_wrangler.py [stepsToRun]
   
 e.g.

 python readNetwork convertPoints identifyTrips createTraj TimeAgg viz
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'convertPoints', 
                'identifyTrips', 
                'createTraj', 
                'timeAgg', 
                'viz'
                ]    

# Vizualization outputs

# date and hour for speed maps
VIZ_DATE = '2009-02-13'  
VIZ_HOUR = '8'

# (date, cab_id, trip_id) for any trajectories to validate
TRAJ_VIZ_SPECS = [('2009-02-13',    '3',   '2'),
                  ('2009-02-13',    '3',  '33'),
                  ('2009-02-13',    '3',  '44'),
                  ('2009-02-13',    '3',  '54'),
                  ('2009-02-13',    '3',  '82'),
                  ('2009-02-13',    '3',  '89'),

                  ('2009-02-13',  '501',   '1'),
                  ('2009-02-13',  '501',  '41'),
                  ('2009-02-13',  '501', '187'),
                  ('2009-02-13',  '501', '201'),
                  ('2009-02-13',  '501', '227'),
                  ('2009-02-13',  '501', '296'),

                  ('2009-02-13',  '649',  '12'), 
                  ('2009-02-13',  '649',  '35'),
                  ('2009-02-13',  '649',  '49'),
                  ('2009-02-13',  '649',  '87'),
                  ('2009-02-13',  '649',  '98'),
                  ('2009-02-13',  '649',  '99'),                  

                  ('2009-02-13', '1349',   '1'),
                  ('2009-02-13', '1349',  '11'),
                  ('2009-02-13', '1349',  '15'),
                  ('2009-02-13', '1349',  '66'),
                  ('2009-02-13', '1349', '135'),
                  ('2009-02-13', '1349', '158'),
                  ('2009-02-13', '1349', '210'),

                  ('2009-02-13', '2813',   '1'),
                  ('2009-02-13', '2813', '536'),
                  ('2009-02-13', '2813', '538'),
                  ('2009-02-13', '2813', '555'),
                  ('2009-02-13', '2813', '572'),
                  ('2009-02-13', '2813', '588')

                  ]

# (cab_id, trip_id) for debugging
TRAJ_DEBUG_SPECS ={(   3,   2),
                  (    3,  33),
                  (    3,  44),
                  (    3,  54),
                  (    3,  82),
                  (    3,  89),
                  
                  (  501,   1),
                  (  501,  41),
                  (  501, 187),
                  (  501, 201),
                  (  501, 227),
                  (  501, 296),
                  
                  (  649,  12), 
                  (  649,  35),
                  (  649,  49),
                  (  649,  87),
                  (  649,  98),
                  (  649,  99),

                  ( 1349,   1),
                  ( 1349,  11),
                  ( 1349,  15),
                  ( 1349,  66),
                  ( 1349, 135),
                  ( 1349, 158),
                  ( 1349, 210),

                  ( 2813,   1),
                  ( 2813, 536),
                  ( 2813, 538),
                  ( 2813, 555),
                  ( 2813, 572),
                  ( 2813, 588)
                  }


# INPUT FILES--change as needed
INPUT_DYNAMEQ_NET_DIR    = "C:/CASA/Data/network/dynameq/validation2010.july19_Sig/Reports/Export"
INPUT_DYNAMEQ_NET_PREFIX = "pb_july19_830p"

RAW_TAXI_FILES =["C:/CASA/Data/taxi/2009-02-13.txt"
                ]
    
# OUTPUT FILES--change as needed
LOGGING_DIR = "C:/CASA/DataExploration"
TAXI_OUTFILE = "C:/CASA/DataExploration/taxi.h5"     
VIZ_OUTFILE  = "C:/CASA/DataExploration/sftaxi.html"    
TRAJ_VIZ_OUTFILE = "C:/CASA/DataExploration/sample_trajectories.html"    
DEBUG_OUTFILE = "C:/CASA/DataExploration/taxi_debug.txt"   


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
    hwynet = None
    
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
        hwynet.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX, logging_dir=LOGGING_DIR) 
        hwynet.initializeSpatialIndex()
        hwynet.initializeShortestPathsBetweenLinks()
        print 'Finished preparing highway network in ', (datetime.datetime.now() - startTime)
        
        startTime = datetime.datetime.now()   
        taxiHelper.openDebugFile(DEBUG_OUTFILE)
        taxiHelper.setDebugCabTripIds(TRAJ_DEBUG_SPECS)
        taxiHelper.createTrajectories(hwynet, TAXI_OUTFILE, 'trip_points', 'trajectories') 
        taxiHelper.closeDebugFile()
        print 'Finished creating taxi trajectories in ', (datetime.datetime.now() - startTime)

    # calculate means and such
    if 'timeAgg' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        taxiHelper.aggregateLinkTravelTimes(TAXI_OUTFILE, 'trajectories', 'link_tt')            
        print 'Finished aggregating link travel times in ', (datetime.datetime.now() - startTime)

    # create network vizualizations
    if 'viz' in STEPS_TO_RUN:
        startTime = datetime.datetime.now()  
        if (hwynet==None): 
            hwynet = HwyNetwork()
            hwynet.readDTANetwork(INPUT_DYNAMEQ_NET_DIR, INPUT_DYNAMEQ_NET_PREFIX, logging_dir=LOGGING_DIR) 
        vizualizer = Visualizer(hwynet, TAXI_OUTFILE)
        
        # network speed maps
        vizualizer.createNetworkPlot(VIZ_OUTFILE, date=VIZ_DATE, hour=VIZ_HOUR)  
        
        # individual trajectory plots
        vizualizer.plotTrajectories(TRAJ_VIZ_OUTFILE, trajSpecs=TRAJ_VIZ_SPECS)  
          
        print 'Finished vizualizing data in ', (datetime.datetime.now() - startTime)
        
    
    print 'Run complete!  Time for a pint!'
    
    