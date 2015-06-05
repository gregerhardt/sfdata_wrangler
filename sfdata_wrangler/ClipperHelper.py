# -*- coding: utf-8 -*-
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
import numpy as np
import datetime


                                    
class ClipperHelper():
    """ 
    Methods used to read Clipper data into a Pandas data frame.  This
    includes definitions of the variables from the raw data, calculating
    computed fields, and some basic clean-up/quality control. 

    """
    
    '''
    The data dictionary for the raw input data is: 
    
     FieldName	        DataType	Example	            Description	                        Notes
     Year	         smallint	2013	            Transaction Year	
     Month	         smallint	10	            Transaction Month (1 is January)	
     CircadianDayOfWeek  smallint	4	            Transaction Day of Week Integer	A day is defined as 3 am to 3 am the following day
     CircadianDayOfWeek_name  char	        Wednesday	    Transaction Day of Week Name	A day is defined as 3 am to 3 am the following day
     RandomWeekID        smallint	6	            Random Integer that Identifies a Unique Day	The Year, Month, DayOfWeek, and RandomWeekID fields uniquely identify a day
     ClipperCardID	 varbinary	D88268EA105â€¦	    Anonymized ClipperÂ® card identifierA random number representing a unique ClipperÂ® card that persists for one circadian day (3 am to 3 am)
     TripSequenceID	 bigint	        2	            Circadian Day Trip Sequence	
     AgencyID	         int	        1	            Transit Agency Integer	
     AgencyName	         char	        AC Transit	    Transit Agency Name	
     PaymentProductID	 int	        119	            Payment Product Integer	
     PaymentProductName  char	        AC Transit Adult    Payment Product Name	
     FareAmount	         money	        0	            Fare	                        Monthly pass holders have a zero fare for each transaction
     TagOnTime_Time	 time	        17:35:00	    Boarding Tag Time	                Times are rounded down to the nerest ten minute interval
     TagOnLocationId	 int	        2	            Boarding Tag Location Integer	
     TagOnLocationName	 char	        Transbay Terminal   Boarding Tag Location Name	
     RouteID	         int	        300	            Route Integer	
     RouteName	         char	        F	            Route Name	                        Not all bus operators transmit route names, e.g. all SF Muni routes are recorded as 'SFM bus'
     TagOffTime_Time	 time	        20:20:00	    Alighting Tag Time	                Times are rounded down to the nearest ten minute interval
     TagOffLocationId	 int	        15	            Alighting Tag Location Integer	For systems that require passengers to tag out of the system
     TagOffLocationName  char	        Millbrae (Caltrain) Alighting Tag Location Name	        For systems that require passengers to tag out of the system
    '''
    
    '''
    These are important calculated fields
    
     MONTH           - combination of month and year, same format as GTFS and so forth
     DOW             - day of week schedule operated: 1-weekday, 2-saturday, 3-sunday
     NUMDAYS         - number of days in the month observed for that DOW
     TIMEDIFF_TAGON  - the time difference (in minutes) from the previous tag on
     TIMEDIFF_TAGOFF - the time difference (in minutes) from the previous tag off
     LinkedTripID    - ID of the linked trip, linking out assumed transfers
     TRANSFER        - the boarding is a transfer fom another route
     From_AgencyID   - AgencyID transfering from 
     From_RouteID    - RouteID transfering from
     From_TagOnLocationID  - TagOnLocationID transfering from
     From_TagOffLocationID - TagOffLocationID transfering from    
     TRANSFERS    - The number of transfers made by this linked trip
     WEIGHT_UNLINKED - A weighting factor to get to the average daily unlinked
                       trips (boardings) for that DOW
     WEIGHT_LINKED - A weighting factor to get to the average daily linke trips
                     for that DOW
    '''
    
       
    # if the time from the last tag on is less than this, then it is 
    # considered a transfer.  Note that a muni transfer fare lasts for 90 min
    TRANSFER_THRESHOLD_TAGON = 90.0   # minutes
    
    # if the time from the last tag off is less than this, then it is 
    # considered a transfer.  
    TRANSFER_THRESHOLD_TAGOFF = 30.0   # minutes
    
    
    def __init__(self):
        """
        Constructor.                 
        """        
        
    
    def processRawData(self, infile, outfile):
        """
        Read SFMuniData, cleans it, processes it, and writes it to an HDF5 file.
        
        infile  - in "raw STP" format
        outfile - output file name in h5 format
        """
        
        print datetime.datetime.now(), 'Converting raw data in file: ', infile
        
        # read the input data
        df = pd.read_csv(infile)
        
        print df.head()
        
        # convert times into pandas datetime formats
        # assume that there is only one year and one month in this file
        year  = df.at[0,'Year']
        month = df.at[0,'Month'] 
        yearMonth = pd.to_datetime(str(100*year + month), format="%Y%m")
        df['MONTH'] = yearMonth
        
        df['TagOnTime_Time']  = pd.to_datetime(df['TagOnTime_Time'], 
            format="%H:%M:%S", exact=False)
        df['TagOffTime_Time'] = pd.to_datetime(df['TagOnTime_Time'], 
            format="%H:%M:%S", exact=False)
                
        # move to scheduled DOW, and calculate number of days
        # TODO deal with holidays
        df['DOW'] = 1 
        df['DOW'] = np.where(df['CircadianDayOfWeek'] == 7, 2, df['DOW'])   # Saturday
        df['DOW'] = np.where(df['CircadianDayOfWeek'] == 1, 3, df['DOW'])   # Sunday       
                        
                        
        # identify transfers
        df['TIMEDIFF_TAGON']  = 9999
        df['TIMEDIFF_TAGOFF'] = 9999
        df['TRANSFER'] = 0
        
        last_row = None 
        for i, row in df.iterrows():
            
            if row['TripSequenceID'] > 1: 
                
                # calculate time from last tag on or off
                timeDiff_tagOn = ((row['TagOnTime_Time'] - 
                    last_row['TagOnTime_Time']).total_seconds()) / 60.0
                    
                if not last_row['TagOffTime_Time'] == pd.NaT: 
                    timeDiff_tagOff = ((row['TagOnTime_Time'] - 
                        last_row['TagOffTime_Time']).total_seconds()) / 60.0
                
                # its a transfer if it's less than the threshold
                if timeDiff_tagOff < self.TRANSFER_THRESHOLD_TAGOFF: 
                    transfer = 1
                elif timeDiff_tagOn < self.TRANSFER_THRESHOLD_TAGON: 
                    transfer = 1
                else:
                    transfer = 0
                
                # only fill in data values if a transfer
                if transfer == 1: 
                    df.at[i, 'TRANSFER']        = transfer
                    df.at[i, 'TIMEDIFF_TAGON']  = timeDiff_tagOn
                    df.at[i, 'TIMEDIFF_TAGOFF'] = timeDiff_tagOff
                    df.at[i, 'From_AgencyID']   = last_row['AgencyID'] 
                    df.at[i, 'From_RouteID']    = last_row['RouteID']
                    df.at[i, 'From_TagOnLocationID']  = last_row['TagOnLocationID']
                    df.at[i, 'From_TagOffLocationID'] = last_row['TagOffLocationID']
                
            last_row = row
        
        # calculate weights
        
        
        # write it to an HDF file
        store = pd.HDFStore(outfile)
        store.append('df', df, data_columns=True)
        store.close()
    
        
