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

sys.path.append('C:\Workspace\SOURCE\sf_taxi\sfdata_wrangler')

from SFMuniDataHelper import SFMuniDataHelper
#from GTFSHelper import GTFSHelper
#from SFMuniDataExpander import SFMuniDataExpander
#from SFMuniDataAggregator import SFMuniDataAggregator
#from MultiModalHelper import MultiModalHelper
#from DemandHelper import DemandHelper
#from MultiModalReporter import MultiModalReporter
from TransitReporter import TransitReporter
#from DemandReporter import DemandReporter
#from ClipperHelper import ClipperHelper


USAGE = r"""

 python sfdata_wrangler.py [stepsToRun]
   
 e.g.

 python sfdata_wrangler clean expand aggUnweighted aggWeighted cleanClipper
 
 Notes: - steps should choose from list of valid steps
        - file names should be edited directly in this script. 
 
"""

    
# VALID STEPS-- list of allowable steps to run
VALID_STEPS = [ 'clean', 
                'expand', 
                'aggregate', 
                'gtfs', 
                'cleanClipper', 
                'multimodal', 
                'demand', 
                'report'
                ]    
                

# INPUT FILES--change as needed
ROUTE_EQUIV = "D:/Input/routeEquiv_20150626.csv"

RAW_STP_FILES =["D:/Input/SFMTA Data/Raw STP Files/0803.stp",
                "D:/Input/SFMTA Data/Raw STP Files/0906.stp",
                "D:/Input/SFMTA Data/Raw STP Files/0912.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1001.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1005.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1009.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1101.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1110.stp",    
                "D:/Input/SFMTA Data/Raw STP Files/1201.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1203.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1206.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1209.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1212.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1303.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1304.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1306.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1308.stp",
                "D:/Input/SFMTA Data/Raw STP Files/1310.stp"
                ]
    
# these should be ordered from old to new, and avoid gaps or overlaps
RAW_GTFS_FILES = [
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20090402_0310_1.zip",  # 20090221 to 20090612 (originally 20090626)
                                                                                           # above file modified to avoid overlap of 13 days
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20091106_0310_1.zip",  # 20090613 to 20091204   (removed trailing sapced from file)
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20100415_0222.zip",  # 20091205 to 20100507
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20100812_0223_1.zip",  # 20100508 to 20100903
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20100908_0243_1.zip",  # 20100904 to 20110102 (originally 20101231)
                                                                                           # above file modified to avoid gap of 2 days
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20110104_0839.zip",  # 20110103 to 20110121
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20110420_0243_1.zip",  # 20110122 to 20110612 (originally 20110610)
                                                                                           # above file modified to avoid gap of 2 days
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20110910_0449.zip",  # 20110613 to 20111014
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20111210_0410.zip",  # 20111015 to 20120120
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20120319_0337_1.zip",  # 20120121 to 20120608 (originally 20120615)
                                                                                           # above file modified to avoid overlap of 6 days
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20120908_0325.zip",  # 20120609 to 20120928
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20130302_0432_1.zip",  # 20120929 to 20130329 (originally 20130322)
                                                                                           # above file modified to avoid gap of 8 days
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20130612_0307.zip",  # 20130330 to 20130628
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20130910_2349.zip",  # 20130629 to 20131025
  "D:/Input/GTFS/san-francisco-municipal-transportation-agency_20140117_0111.zip"   # 20131026 to 20140131  
  ]


# these should be ordered from old to new, and avoid gaps or overlaps
BART_GTFS_FILES = [
    "C:/CASA/Data/BART/GTFS/bart-archiver_20090701_0208_1.zip",        # 20070101,20090630 (originally 20091231)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20090728_0211_1.zip",        # 20090701,20090913 (originally 20091231)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20091006_0210_1.zip",        # 20090914,20100912 (originally 20101231)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20101112_0839_1.zip",        # 20100913,20110218 (originally 20111231)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20120210_0412_1.zip",        # 20110219,20120630 (originally 20130101)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20120820_0458_1.zip",        # 20120701,20120909 (originally 20140101)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20130830_1519_1.zip",        # 20120910,20131231 (originally 20140101)
    "C:/CASA/Data/BART/GTFS/bart-archiver_20140109_0110_1.zip",        # 20140101,20141121 (originally 20151231)
    "C:/CASA/Data/BART/GTFS/bay-area-rapid-transit_20150804_0108_1.zip", # 20141122,20150913 (originally 20160101)
    "C:/CASA/Data/BART/GTFS/bay-area-rapid-transit_20150930_1457_1.zip"  # 20150914,20170101 (modified exception dates to be in range)
    ]


# we will append the year to this
BART_ENTRY_EXIT_DIR = "C:/CASA/Data/BART/ridership_"    


RAW_CLIPPER_FILES =["D:/Input/Clipper/2013_-_3_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_5_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_6_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_7_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_8_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_9_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_10_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2013_-_11_Anonymous_Clipper.csv",                    
                    "D:/Input/Clipper/2013_-_12_Anonymous_Clipper.csv",  
                                      
                    "D:/Input/Clipper/2014_-_1_Anonymous_Clipper.csv",
                    "D:/Input/Clipper/2014_-_2_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_3_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_4_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_5_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_6_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_7_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_8_Anonymous_Clipper.csv", 
                    "D:/Input/Clipper/2014_-_9_Anonymous_Clipper.csv"                    
                   ]

CENSUS2000_DIR = "C:/CASA/Data/Census/Census2000/"
CENSUS2010_FILE = "C:/CASA/Data/Census/Census2010/DP01/DEC_10_SF1_SF1DP1_with_ann.csv" 

CENSUS_POPEST_PRE2010_FILE  = "C:/CASA/Data/Census/AnnualPopulationEstimates/2000to2010/CO-EST00INT-TOT.csv"
CENSUS_POPEST_POST2010_FILE = "C:/CASA/Data/Census/AnnualPopulationEstimates/post2010/PEP_2014_PEPANNRES_with_ann.csv"

ACS_DIR = "C:/CASA/Data/Census/ACS/Tables/"

HOUSING_COMPLETIONS_FILES = ["C:/CASA/Data/HousingInventory/sfhousingcompletesthrough2011.csv", 
                             "C:/CASA/Data/HousingInventory/2012_Housing_Inventory.csv",  
                             "C:/CASA/Data/HousingInventory/2013_Housing_Inventory.csv",  
                             "C:/CASA/Data/HousingInventory/2014_Housing_Inventory.csv"
                             ] 


QCEW_DIR = "D:/Input/QCEW/"

LODES_DIR = "D:/Input/Census/LEHD/LODES/CA/"
LODES_XWALK_FILE= "D:/Input/Census/LEHD/LODES/CA/ca_xwalk.csv"

FUEL_COST_FILE = "C:/CASA/Data/AutoOpCosts/FuelCost/PET_PRI_GND_A_EPM0_PTE_DPGAL_M.xls"
FLEET_EFFICIENCY_FILE = "C:/CASA/Data/AutoOpCosts/FleetEfficiency/table_04_23_0.csv"
MILEAGE_RATE_FILE = "C:/CASA/Data/AutoOpCosts/IRS/StandardMileageRates.csv"

PARKING_RATE_FILE = "C:/CASA/Data/ParkingRateSurveys/CBDParkingRateSurveys.csv"

TOLL_FILE = "C:/CASA/Data/Tolls/TollSchedules.csv"

CASH_FARE_FILE = "C:/CASA/Data/TransitFares/TransitCashFares.csv"

CPI_FILE       = "C:/CASA/Data/CPI/SeriesReport-20150908105105_8887b6.xlsx"

TRANSIT_ANNUAL_DIR = "C:/CASA/Data/TransitStatisticalSummary"

FIPS = [('06001' , 'Alameda County', 'AC'), 
        ('06013' , 'Contra Costa County', 'CCC'), 
        ('06075' , 'San Francisco County', 'SFC'), 
        ('06081' , 'San Mateo County', 'SMC')]

# OUTPUT FILES--change as needed
CLEANED_OUTFILE       = "D:/Output/sfmuni_cleaned.h5"    

EXPANDED_TRIP_OUTFILE = "D:/Output/sfmuni_expanded_trip_YYYY.h5"    
EXPANDED_TS_OUTFILE   = "D:/Output/sfmuni_expanded_ts_YYYY.h5" 

DAILY_TRIP_OUTFILE = "D:/Output/sfmuni_daily_trip.h5"
DAILY_TS_OUTFILE   = "D:/Output/sfmuni_daily_ts.h5"

MONTHLY_TRIP_OUTFILE = "E:\Transit_Casa\Output/sfmuni_monthly_trip.h5"
MONTHLY_TS_OUTFILE   = "E:\Transit_Casa\Output/sfmuni_monthly_ts.h5"

GTFS_OUTFILE = "C:/CASA/PerformanceReports/gtfs.h5"
CLIPPER_OUTFILE = "D:/Output/clipper3.h5"
DEMAND_OUTFILE = "E:\Transit_Casa\Alex\Output\Performence Report/drivers_of_demand.h5"
MULTIMODAL_OUTFILE = "E:\Transit_Casa\Alex\Output\Performence Report/multimodal.h5"

MULTIMODAL_REPORT_XLSFILE = "E:\Transit_Casa\Alex\Output\Performence Report/MultiModalReport.xlsx"
DEMAND_REPORT_XLSFILE = "E:\Transit_Casa\Alex\Output\Performence Report/DriversOfDemandReport.xlsx"
MUNI_REPORT_XLSFILE = "E:\Transit_Casa\Alex\Output\Performence Report/MuniPerformanceReport.xlsx"
REPORT_ROUTEPLOTS = "E:\Transit_Casa\Alex\Output\Performence Report/RoutePlots"

MUNI_ESTIMATION_FILE = "C:/CASA/ModelEstimation/PostViva/data/MuniForecastFile.csv"
BART_ESTIMATION_FILE = "C:/CASA/ModelEstimation/PostViva/data/BARTForecastFile.csv"


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
    if 'clean' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()  
        sfmuniHelper = SFMuniDataHelper()
        sfmuniHelper.readRouteEquiv(ROUTE_EQUIV) 
        for infile in RAW_STP_FILES: 
            sfmuniHelper.processRawData(infile, CLEANED_OUTFILE)
        print ('Finished cleaning SFMuni data in ', (datetime.datetime.now() - startTime))

    # process GTFS data, and join AVL/APC data to it, also aggregate trip_stops to trips
    if 'expand' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        sfmuniExpander = SFMuniDataExpander(sfmuni_file=CLEANED_OUTFILE, 
                                trip_outfile=EXPANDED_TRIP_OUTFILE, 
                                ts_outfile=EXPANDED_TS_OUTFILE, 
                                daily_trip_outfile=DAILY_TRIP_OUTFILE, 
                                daily_ts_outfile=DAILY_TS_OUTFILE, 
                                dow=[1], 
                                startDate='2000-01-01')
        for infile in RAW_GTFS_FILES: 
            sfmuniExpander.expandAndWeight(infile)   
        print ('Finished expanding to GTFS in ', (datetime.datetime.now() - startTime))

    # aggregate to monthly totals
    if 'aggregate' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        aggregator = SFMuniDataAggregator()
        aggregator.aggregateTripsToMonths(DAILY_TRIP_OUTFILE, MONTHLY_TRIP_OUTFILE)
        aggregator.aggregateTripStopsToMonths(DAILY_TS_OUTFILE, MONTHLY_TS_OUTFILE)
        print ('Finished aggregations in ', (datetime.datetime.now() - startTime)) 


    # process GTFS schedule data.  
    if 'gtfs' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        gtfsHelper = GTFSHelper() 
        gtfsHelper.processFiles(RAW_GTFS_FILES, GTFS_OUTFILE, 'sfmuni')
        gtfsHelper.processFiles(BART_GTFS_FILES, GTFS_OUTFILE, 'bart')
        print ('Finished processing GTFS data ', (datetime.datetime.now() - startTime) )
        

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

        #demandHelper.processCensusPopulationEstimates(CENSUS_POPEST_PRE2010_FILE, 
        #                                              CENSUS_POPEST_POST2010_FILE, 
        #                                              FIPS, 
        #                                              DEMAND_OUTFILE)      
        
        demandHelper.processCensusSampleData(ACS_DIR, CENSUS2000_DIR, FIPS, CPI_FILE, DEMAND_OUTFILE)  
        
        #demandHelper.processHousingUnitsData(HOUSING_COMPLETIONS_FILES, CENSUS2010_FILE, DEMAND_OUTFILE)          

        #demandHelper.processQCEWData(QCEW_DIR, FIPS, CPI_FILE, DEMAND_OUTFILE)  

        #demandHelper.processLODES(LODES_DIR, 'WAC', LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
        #demandHelper.processLODES(LODES_DIR, 'RAC', LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
        #demandHelper.processLODES(LODES_DIR, 'OD',  LODES_XWALK_FILE, FIPS, DEMAND_OUTFILE) 
                                                             
        #demandHelper.processAutoOpCosts(FUEL_COST_FILE, FLEET_EFFICIENCY_FILE, 
        #                           MILEAGE_RATE_FILE, CPI_FILE, DEMAND_OUTFILE)

        #demandHelper.processParkingCosts(PARKING_RATE_FILE, CPI_FILE, DEMAND_OUTFILE)

        #demandHelper.processTollCosts(TOLL_FILE, CPI_FILE, DEMAND_OUTFILE)
        

        print ('Finished processing drivers of demand data ', (datetime.datetime.now() - startTime) )
        
        
    # process multimodal data  
    if 'multimodal' in STEPS_TO_RUN: 
        startTime = datetime.datetime.now()   
        mmHelper = MultiModalHelper()

        #mmHelper.processAnnualTransitData(TRANSIT_ANNUAL_DIR, CPI_FILE, MULTIMODAL_OUTFILE)    
        #mmHelper.processMonthlyTransitData(CPI_FILE, MULTIMODAL_OUTFILE)      
        #mmHelper.extrapolateMonthlyServiceMiles(GTFS_OUTFILE, MULTIMODAL_OUTFILE) 
        #mmHelper.processTransitFares(CASH_FARE_FILE, CPI_FILE, MULTIMODAL_OUTFILE)
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

        #reporter.writeDemandReport(DEMAND_REPORT_XLSFILE, FIPS)

        #reporter.writeMultiModalReport(MULTIMODAL_REPORT_XLSFILE, fips='06075')

        # demand data only for SF county
        #reporter.writeSFMuniEstimationFile(MUNI_ESTIMATION_FILE, fips='06075')

        # demand data for all counties
        #reporter.writeBARTEstimationFile(BART_ESTIMATION_FILE, FIPS)
        
        
        #reporter.createRoutePlot(REPORT_ROUTEPLOTS, 
                                # months=('2009-09-01', '2013-09-01'), 
                             #    dow=1, 
                              #   tod='Daily', 
                              #   route_short_name='22', 
                              #   dir=1)

        
        print ('Finished performance reports in ', (datetime.datetime.now() - startTime) )

    print ('Run complete!  Time for a pint!')
    
    