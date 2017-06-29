
# allows python3 style print function
from __future__ import print_function

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

sys.path.append('D:/WORKSPACE/sfdata_wrangler/sfdata_wrangler')

from SFMuniDataHelper import SFMuniDataHelper
from GTFSHelper import GTFSHelper
from SFMuniDataExpander import SFMuniDataExpander
from SFMuniDataAggregator import SFMuniDataAggregator
from MultiModalHelper import MultiModalHelper
from DemandHelper import DemandHelper
from TransitReporter import TransitReporter
from ClipperHelper import ClipperHelper


USAGE = r"""

 python sfdata_wrangler.py [stepsToRun]
   
 e.g.

 python sfdata_wrangler clean expand aggUnweighted aggWeighted cleanClipper
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'clean1', 
                'clean2', 
                'gtfs', 
                'expand', 
                'aggregate', 
                'cleanClipper', 
                'multimodal', 
                'demand', 
                'report'
                ]    
                

# INPUT FILES--change as needed
ROUTE_EQUIV = "D:/RUNS/sfdata_wrangler2/routeEquiv_20170621.csv"

RAW_STP_FILES =[#"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/0803.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/0906.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/0912.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1001.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1005.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1009.stp",
                #
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1101.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1110.stp",    
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1201.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1203.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1206.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1209.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1212.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1303.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1304.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1306.stp",
                #
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1308.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1310_new.stp", 
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1402.stp", 
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1404.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1406.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1407.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1410.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1411.stp",
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1501.stp",
                
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1504.stp",
                
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1509.stp", 				
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1604.stp", 				
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1606.stp", 
                #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1608.stp", 
                "D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1702.stp", 
                "D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/SFMTA Data/Raw STP Files/1706.stp", 
                ]
    
# these should be ordered from old to new, and avoid gaps or overlaps
RAW_GTFS_FILES = [
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20090402_0310_1.zip",  # 20090221 to 20090612 (originally 20090626)
  #                                                                                         # above file modified to avoid overlap of 13 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20091106_0310_1.zip",  # 20090613 to 20091204   (removed trailing sapced from file)
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100415_0222.zip",  # 20091205 to 20100507
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100812_0223_1.zip",  # 20100508 to 20100903
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20100908_0243_1.zip",  # 20100904 to 20110102 (originally 20101231)
  #                                                                                         # above file modified to avoid gap of 2 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110104_0839.zip",  # 20110103 to 20110121
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110420_0243_1.zip",  # 20110122 to 20110612 (originally 20110610)
  #                                                                                         # above file modified to avoid gap of 2 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20110910_0449.zip",  # 20110613 to 20111014
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20111210_0410.zip",  # 20111015 to 20120120
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20120319_0337_1.zip",  # 20120121 to 20120608 (originally 20120615)
  #                                                                                         # above file modified to avoid overlap of 6 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20120908_0325.zip",  # 20120609 to 20120928
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130302_0432_1.zip",  # 20120929 to 20130329 (originally 20130322)
  #                                                                                         # above file modified to avoid gap of 8 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130612_0307.zip",  # 20130330 to 20130628
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20130910_2349_1.zip",  # 20130629 to 20131025  
  #                                                                                         # above file modified to change '016X' to '16X' in ROUTE_SHORT_NAME
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20140117_0111.zip",   # 20131026 to 20140131  
  #
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20140319.zip",          # 20140201 to 20140411
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20140416_0134.zip",     # 20140412 to 20140606
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20140611_0111.zip",     # 20140607 to 20140621
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20141007_0221.zip",     # 20140621 to 20141024
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20141029_0132_1.zip",   # 20141025 to 20141121 (originally 20141219)
  #                                                                                                                                         # above file modified to avoid overlap of 27 days  
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20141220_0202.zip",     # 20141122 to 20150130
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20150227_0144.zip",     # 20150131 to 20150424
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20150809_0207.zip",     # 20150425 to 20150925
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20151216_1446_1.zip",   # 20150926 to 20160212 (originally 20160311)
  #                                                                                                                                         # above file modified to avoid overlap of 19 days
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20160307.zip",          # 20160213 to 20160422
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20160506.zip",          # 20160423 to 20160603
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20160724.zip",          # 20160604 to 20160812
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20170215.zip",          # 20160813 to 20170224
  #"D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20170410.zip",          # 20170225 to 20170602
  "D:/OneDrive - University of Kentucky/SF-TNC/Data/MUNI/GTFS/san-francisco-municipal-transportation-agency_20170606.zip",          # 20170603 to 20170811

  ]


# these should be ordered from old to new, and avoid gaps or overlaps
BART_GTFS_FILES = [
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20090701_0208_1.zip",        # 20070101,20090630 (originally 20091231)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20090728_0211_1.zip",        # 20090701,20090913 (originally 20091231)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20091006_0210_1.zip",        # 20090914,20100912 (originally 20101231)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20101112_0839_1.zip",        # 20100913,20110218 (originally 20111231)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20120210_0412_1.zip",        # 20110219,20120630 (originally 20130101)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20120820_0458_1.zip",        # 20120701,20120909 (originally 20140101)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20130830_1519_1.zip",        # 20120910,20131231 (originally 20140101)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bart-archiver_20140109_0110_1.zip",        # 20140101,20141121 (originally 20151231)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bay-area-rapid-transit_20150804_0108_1.zip", # 20141122,20150913 (originally 20160101)
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bay-area-rapid-transit_20151106_1.zip",       # 20150914,20170324 (originally 20170101) - schedule change confirmed on 9/15/2015
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bay-area-rapid-transit_20170427_1.zip",       # 20170325,20170611 (originally 20180101) - Warm Springs exension opens on 3/25/2017
    "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/GTFS/bay-area-rapid-transit_20170615.zip",         # 20170612,20180701  - possible Warm Springs schedule adjustment?   
    ]


# we will append the year to this
BART_ENTRY_EXIT_DIR = "D:/OneDrive - University of Kentucky/SF-TNC/Data/BART/ridership_"    


RAW_CLIPPER_FILES =["D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_3_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_5_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_6_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_7_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_8_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_9_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_10_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_11_Anonymous_Clipper.csv",                    
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2013_-_12_Anonymous_Clipper.csv",  
                                      
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_1_Anonymous_Clipper.csv",
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_2_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_3_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_4_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_5_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_6_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_7_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_8_Anonymous_Clipper.csv", 
                    "D:/OneDrive - University of Kentucky/SF-TNC/Data/Clipper/2014_-_9_Anonymous_Clipper.csv"                    
                   ]

CENSUS2000_DIR = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/Census2000/"
CENSUS2010_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/Census2010/DP01/DEC_10_SF1_SF1DP1_with_ann.csv" 

CENSUS_POPEST_PRE2010_FILE  = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/AnnualPopulationEstimates/2000to2010/CO-EST00INT-TOT.csv"
CENSUS_POPEST_POST2010_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/AnnualPopulationEstimates/post2010/PEP_2016_PEPANNRES_with_ann.csv"

ACS_DIR = "C:/CASA/Data/Census/ACS/Tables/"

HOUSING_COMPLETIONS_FILES = ["D:/OneDrive - University of Kentucky/SF-TNC/Data/HousingInventory/sfhousingcompletesthrough2011.csv", 
                             "D:/OneDrive - University of Kentucky/SF-TNC/Data/HousingInventory/2012_Housing_Inventory.csv",  
                             "D:/OneDrive - University of Kentucky/SF-TNC/Data/HousingInventory/2013_Housing_Inventory.csv",  
                             "D:/OneDrive - University of Kentucky/SF-TNC/Data/HousingInventory/2014_Housing_Inventory.csv",  
                             "D:/OneDrive - University of Kentucky/SF-TNC/Data/HousingInventory/2015_Housing_Inventory.csv"
                             ] 


QCEW_DIR = "D:/OneDrive - University of Kentucky/SF-TNC/Data/QCEW/"

LODES_DIR = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/LEHD/LODES/CA/"
LODES_XWALK_FILE= "D:/OneDrive - University of Kentucky/SF-TNC/Data/Census/LEHD/LODES/CA/ca_xwalk.csv"

FUEL_COST_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/AutoOpCosts/FuelCost/PET_PRI_GND_A_EPM0_PTE_DPGAL_M.xls"
FLEET_EFFICIENCY_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/AutoOpCosts/FleetEfficiency/table_04_23_4.csv"
MILEAGE_RATE_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/AutoOpCosts/IRS/StandardMileageRates.csv"

PARKING_RATE_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/ParkingRateSurveys/CBDParkingRateSurveys.csv"

TOLL_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/Tolls/TollSchedules.csv"

CASH_FARE_FILE = "D:/OneDrive - University of Kentucky/SF-TNC/Data/TransitFares/TransitCashFares.csv"

CPI_FILE       = "D:/OneDrive - University of Kentucky/SF-TNC/Data/CPI/SeriesReport-20170629154705_55d952.xlsx"

TRANSIT_ANNUAL_DIR = "D:/OneDrive - University of Kentucky/SF-TNC/Data/TransitStatisticalSummary"

FIPS = [('06001' , 'Alameda County', 'AC'), 
        ('06013' , 'Contra Costa County', 'CCC'), 
        ('06075' , 'San Francisco County', 'SFC'), 
        ('06081' , 'San Mateo County', 'SMC')]

# OUTPUT FILES--change as needed
CLEANED_OUTFILES_STEP1  = ["D:/RUNS/sfdata_wrangler2/out/sfmuni_cleaned_part1.h5", 
                           "D:/RUNS/sfdata_wrangler2/out/sfmuni_cleaned_part2.h5", 
                           "D:/RUNS/sfdata_wrangler2/out/sfmuni_cleaned_part3.h5", 
                           "D:/RUNS/sfdata_wrangler2/out/sfmuni_cleaned_part4.h5"
                           ]

CLEANED_OUTFILES_STEP2 = "D:/RUNS/sfdata_wrangler2/out/sfmuni_cleaned_YYYY.h5"    

NOMATCH_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/cleaned_nomatch_"   

EXPANDED_TRIP_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/sfmuni_expanded_trip_YYYY.h5"    
EXPANDED_TS_OUTFILE   = "D:/RUNS/sfdata_wrangler2/out/sfmuni_expanded_ts_YYYY.h5" 

DAILY_TRIP_OUTFILES = [ "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2009.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2010.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2011.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2012.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2013.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2014.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2015.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2016.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_trip_2017.h5"
                       ]

DAILY_TS_OUTFILES   = [ "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2009.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2010.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2011.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2012.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2013.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2014.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2015.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2016.h5", 
                        "D:/RUNS/sfdata_wrangler2/out/sfmuni_daily_ts_2017.h5"
                       ]

MONTHLY_TRIP_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/sfmuni_monthly_trip.h5"
MONTHLY_TS_OUTFILE   = "D:/RUNS/sfdata_wrangler2/out/sfmuni_monthly_ts.h5"

GTFS_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/gtfs.h5"
CLIPPER_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/clipper3.h5"
DEMAND_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/drivers_of_demand.h5"
MULTIMODAL_OUTFILE = "D:/RUNS/sfdata_wrangler2/out/multimodal.h5"

MULTIMODAL_REPORT_XLSFILE = "D:/RUNS/sfdata_wrangler2/out/MultiModalReport.xlsx"
DEMAND_REPORT_XLSFILE = "D:/RUNS/sfdata_wrangler2/out/DriversOfDemandReport.xlsx"
MUNI_REPORT_XLSFILE = "D:/RUNS/sfdata_wrangler2/out/MuniPerformanceReport.xlsx"
REPORT_ROUTEPLOTS = "D:/RUNS/sfdata_wrangler2/out/RoutePlots.html"

MUNI_ESTIMATION_FILE = "D:/RUNS/sfdata_wrangler2/out/MuniForecastFile.csv"
BART_ESTIMATION_FILE = "D:/RUNS/sfdata_wrangler2/out/BARTForecastFile.csv"


# main function call

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print (USAGE)
        print ('Valid steps include: ', VALID_STEPS)
        sys.exit(2)

    STEPS_TO_RUN = sys.argv[1:]
    for step in STEPS_TO_RUN: 
        if not (step in VALID_STEPS): 
            print (step, ' is not a valid step to run')
            print ('Valid steps include: ', VALID_STEPS)
            sys.exit(2)

    # convert the AVL/APC data
    if 'clean1' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()  
        sfmuniHelper = SFMuniDataHelper()
        sfmuniHelper.readRouteEquiv(ROUTE_EQUIV) 
        for infile in RAW_STP_FILES: 
            sfmuniHelper.processRawData(infile, CLEANED_OUTFILES_STEP1[0])
        print ('Finished cleaning step 1 SFMuni data in ', (datetime.datetime.now() - startTime))

    # update RouteEquiv and write to separate files by year
    if 'clean2' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()  
        sfmuniHelper = SFMuniDataHelper()
        sfmuniHelper.readRouteEquiv(ROUTE_EQUIV) 
        for infile in CLEANED_OUTFILES_STEP1: 
            sfmuniHelper.cleanPart2(infile, CLEANED_OUTFILES_STEP2)
        print ('Finished cleaning step 2 SFMuni data in ', (datetime.datetime.now() - startTime))
        
    # process GTFS schedule data.  
    if 'gtfs' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        gtfsHelper = GTFSHelper() 
        gtfsHelper.processFiles(RAW_GTFS_FILES, GTFS_OUTFILE, 'sfmuni', use_shape_dist=False)
        gtfsHelper.processFiles(BART_GTFS_FILES, GTFS_OUTFILE, 'bart', use_shape_dist=True)
        print ('Finished processing GTFS data ', (datetime.datetime.now() - startTime) )
        
    # process GTFS data, and join AVL/APC data to it, also aggregate trip_stops to trips
    if 'expand' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        sfmuniExpander = SFMuniDataExpander(gtfs_outfile=GTFS_OUTFILE, 
                                sfmuni_file=CLEANED_OUTFILES_STEP2, 
                                trip_outfile=EXPANDED_TRIP_OUTFILE, 
                                ts_outfile=EXPANDED_TS_OUTFILE, 
                                daily_trip_outfile=DAILY_TRIP_OUTFILES[0], 
                                daily_ts_outfile=DAILY_TS_OUTFILES[0], 
                                dow=[1], 
                                startDate='1900-01-01', 
                                endDate='2100-12-31')
        for gtfs_infile in RAW_GTFS_FILES: 
            sfmuniExpander.expandAndWeight(gtfs_infile)
        sfmuniExpander.closeStores()
        print ('Finished expanding to GTFS in ', (datetime.datetime.now() - startTime))

    # aggregate to monthly totals
    if 'aggregate' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        aggregator = SFMuniDataAggregator()
        for daily_file in DAILY_TRIP_OUTFILES: 
            aggregator.aggregateTripsToMonths(daily_file, MONTHLY_TRIP_OUTFILE)
            
        for daily_file in DAILY_TS_OUTFILE: 
            aggregator.aggregateTripStopsToMonths(daily_file, MONTHLY_TS_OUTFILE)
            
        print ('Finished aggregations in ', (datetime.datetime.now() - startTime)) 



    # process Clipper data.  
    if 'cleanClipper' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        clipperHelper = ClipperHelper()
        for infile in RAW_CLIPPER_FILES: 
            clipperHelper.processRawData(infile, CLIPPER_OUTFILE)   
        print ('Finished processing Clipper data ', (datetime.datetime.now() - startTime) )
        
        
    # process drivers of demand data.  
    if 'demand' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        demandHelper = DemandHelper()

        demandHelper.processCensusPopulationEstimates(CENSUS_POPEST_PRE2010_FILE, 
                                                      CENSUS_POPEST_POST2010_FILE, 
                                                      FIPS, 
                                                      DEMAND_OUTFILE)      
        
        demandHelper.processCensusSampleData(ACS_DIR, CENSUS2000_DIR, FIPS, CPI_FILE, DEMAND_OUTFILE)  
        
        demandHelper.processHousingUnitsData(HOUSING_COMPLETIONS_FILES, CENSUS2010_FILE, DEMAND_OUTFILE)          

        demandHelper.processQCEWData(QCEW_DIR, FIPS, CPI_FILE, DEMAND_OUTFILE)  

        demandHelper.processLODES(LODES_DIR, 'WAC', LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
        demandHelper.processLODES(LODES_DIR, 'RAC', LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
        demandHelper.processLODES(LODES_DIR, 'OD',  LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
                                                             
        demandHelper.processAutoOpCosts(FUEL_COST_FILE, FLEET_EFFICIENCY_FILE, 
                                   MILEAGE_RATE_FILE, CPI_FILE, DEMAND_OUTFILE)

        demandHelper.processParkingCosts(PARKING_RATE_FILE, CPI_FILE, DEMAND_OUTFILE)

        demandHelper.processTollCosts(TOLL_FILE, CPI_FILE, DEMAND_OUTFILE)
        

        print ('Finished processing drivers of demand data ', (datetime.datetime.now() - startTime) )
        
        
    # process multimodal data  
    if 'multimodal' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        mmHelper = MultiModalHelper()

        mmHelper.processAnnualTransitData(TRANSIT_ANNUAL_DIR, CPI_FILE, MULTIMODAL_OUTFILE)    
        mmHelper.processMonthlyTransitData(CPI_FILE, MULTIMODAL_OUTFILE)      
        mmHelper.extrapolateMonthlyServiceMiles(GTFS_OUTFILE, MULTIMODAL_OUTFILE) 
        mmHelper.processTransitFares(CASH_FARE_FILE, CPI_FILE, MULTIMODAL_OUTFILE)
        mmHelper.processBARTEntryExits(BART_ENTRY_EXIT_DIR, MULTIMODAL_OUTFILE)


    # create performance reports
    if 'report' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
                
        reporter = TransitReporter(trip_file=MONTHLY_TRIP_OUTFILE, 
                                   ts_file=MONTHLY_TS_OUTFILE, 
                                   demand_file=DEMAND_OUTFILE,
                                   gtfs_file=GTFS_OUTFILE, 
                                   multimodal_file=MULTIMODAL_OUTFILE)
        reporter.writeSystemReport(MUNI_REPORT_XLSFILE, fips='06075', dow=1)

        reporter.writeDemandReport(DEMAND_REPORT_XLSFILE, FIPS)

        reporter.writeMultiModalReport(MULTIMODAL_REPORT_XLSFILE, fips='06075')

        # demand data only for SF county
        reporter.writeSFMuniEstimationFile(MUNI_ESTIMATION_FILE, fips='06075')

        # demand data for all counties
        reporter.writeBARTEstimationFile(BART_ESTIMATION_FILE, FIPS)
        
        
        #reporter.createRoutePlot(REPORT_ROUTEPLOTS, 
        #                         months=('2009-07-01', '2010-07-01'), 
        #                         dow=1, 
        #                         tod='0600-0859', 
        #                         route_short_name=1, 
        #                         dir=1)

        
        print('Finished performance reports in ', (datetime.datetime.now() - startTime))

    print('Run complete!  Time for a pint!')
    
    