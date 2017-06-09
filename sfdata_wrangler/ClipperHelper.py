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


def applyLateNightOffset(dateTime):        
    """
    The transit operating day runs from 3 am to 3 am.  
    So if the time given is between midnight and 2:59 am, increment the
    day on that time tag to be one day later.  This way, we can subtract
    times and get the proper difference. 
    """
    
    if (dateTime.hour < 3): 
        return (dateTime + pd.DateOffset(days=1))
    else: 
        return dateTime   

    
def clipperWeights(dow):
    """
    Calculate a weights for expanding the data to average weekday/saturday/sunday
    conditions.
    
    dow - day of week (1-weekday, 2-saturday, 3-sunday)
        
     currently , the weights account for two factors used in creating/obfuscating
     the data: 
        c.  For each day, we sample 50 percent of cards.
            --> this leads to a weight of 2.0 to scale up
        
        d.  We assign each Sunday within each month a random number between 
            1 and 10; we then randomly select three Sundays (retaining the 
            random, identifying integer) and discard the fourth (and fifth, 
            if relevant).  We repeat this procedure for each day of the week.
            --> this leads to a weight of 1/3 for saturdays and sundays, 
                and 1/15 for weekdays to get to an average condition
                                        
    """
    
    if dow == 1: 
        return 2.0 / 15.0
    elif dow == 2: 
        return 2.0 / 3.0
    elif dow == 3: 
        return 2.0 / 3.0
        

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
     CircadianDayOfWeek_name  char	Wednesday	    Transaction Day of Week Name	A day is defined as 3 am to 3 am the following day
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
        
        print(datetime.datetime.now(), 'Converting raw data in file: ', infile)
        
        # read the input data
        df = pd.read_csv(infile)
        
        print(datetime.datetime.now(), '  calculate')
        
        # make the tables format nicer in terms of the strings used
        df['AgencyName'] = df['AgencyName'].apply(str.strip)

        # convert times into pandas datetime formats
        # assume that there is only one year and one month in this file
        year  = df.at[0,'Year']
        month = df.at[0,'Month'] 
        yearMonth = str(100*year + month) + '-'
        df['MONTH'] = pd.to_datetime(yearMonth, format="%Y%m-")
                
        df['TagOnTime_Time']  = pd.to_datetime(yearMonth + df['TagOnTime_Time'], 
            format="%Y%m-%H:%M:%S", exact=False)
        df['TagOffTime_Time'] = pd.to_datetime(yearMonth + df['TagOffTime_Time'], 
            format="%Y%m-%H:%M:%S", exact=False)
            
        # deal with the operating day starting and ending at 3 am
        df['TagOnTime_Time']  = df['TagOnTime_Time'].apply(applyLateNightOffset)
        df['TagOffTime_Time'] = df['TagOffTime_Time'].apply(applyLateNightOffset)
                
        # move to scheduled DOW, and calculate number of days
        # TODO deal with holidays
        df['DOW'] = 1 
        df['DOW'] = np.where(df['CircadianDayOfWeek'] == 7, 2, df['DOW'])   # Saturday
        df['DOW'] = np.where(df['CircadianDayOfWeek'] == 1, 3, df['DOW'])   # Sunday       
                
        # infer the mode
        df['MODE'] = 'Bus'
        df['MODE'] = np.where(df['AgencyName'] == 'BART', 'Rapid Transit', df['MODE'])
        df['MODE'] = np.where(df['AgencyName'] == 'Caltrain', 'Commuter Rail', df['MODE'])
        df['MODE'] = np.where(df['AgencyName'] == 'Golden Gate Ferry', 'Ferry', df['MODE'])
        df['MODE'] = np.where(df['AgencyName'] == 'WETA', 'Ferry', df['MODE'])
        
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='VTA', df['RouteName']=='LRV'), 'Light Rail', df['MODE'])
        
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='CC59'), 'Cable Car', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='CC60'), 'Cable Car', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='CC61'), 'Cable Car', df['MODE'])
        
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['TagOnLocationName']!='SFM bus'), 'Light Rail', df['MODE'])
        
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='F'), 'Light Rail', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='J'), 'Light Rail', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='K'), 'Light Rail', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='L'), 'Light Rail', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='M'), 'Light Rail', df['MODE'])
        df['MODE'] = np.where(np.logical_and(df['AgencyName']=='SF Muni', df['RouteName']=='N'), 'Light Rail', df['MODE'])
        
        # sort 
        sortColumns = ['ClipperCardID', 'TripSequenceID']
        df.sort(sortColumns, inplace=True)               
                        
        # identify transfers
        print(datetime.datetime.now(), '  loop')
        df['TIMEDIFF_TAGON']  = 9999
        df['TIMEDIFF_TAGOFF'] = 9999
        df['TRANSFER'] = 0
        df['LINKED_TRIP_ID'] = 1
        
        last_row = None 
        firstRow = True
        for i, row in df.iterrows():
            
            if firstRow: 
                firstRow = False
                linkedTripId = 1
            
            # keep track of the ID for linked trips
            elif row['ClipperCardID'] != last_row['ClipperCardID']: 
                linkedTripId = 1
                
            else:                                 
                # calculate time from last tag on or off
                timeDiff_tagOn = ((row['TagOnTime_Time'] - 
                    last_row['TagOnTime_Time']).total_seconds()) / 60.0
                    
                # its a transfer if it's less than the threshold
                if timeDiff_tagOn < self.TRANSFER_THRESHOLD_TAGON: 
                    df.at[i, 'TRANSFER']        = 1
                    df.at[i, 'TIMEDIFF_TAGON']  = timeDiff_tagOn
                    df.at[i, 'From_AgencyID']   = last_row['AgencyID'] 
                    df.at[i, 'From_MODE']       = last_row['MODE'] 
                    df.at[i, 'From_RouteID']    = last_row['RouteID']
                    df.at[i, 'From_TagOnLocationID']  = last_row['TagOnLocationID']
                    df.at[i, 'From_TagOffLocationID'] = last_row['TagOffLocationID']
                else:
                    df.at[i, 'TRANSFER']        = 0                
                    linkedTripId += 1
                    
                df.at[i,'LINKED_TRIP_ID'] = linkedTripId
                                
            last_row = row
        
        # determine how many transfers are on each linked trip
        #print(datetime.datetime.now(), '  transform')
        selected = df[['ClipperCardID', 'LINKED_TRIP_ID','TRANSFER']]
        transformed = selected.groupby(['ClipperCardID', 'LINKED_TRIP_ID']).transform(sum)
        df['LINKED_TRANSFERS'] = transformed['TRANSFER']
        
        print(datetime.datetime.now(), '  calculate weights') 
        # these will represent average ridership by DOW (weekday, saturday, sunday)
        #TODO - update to match external boarding counts...        
        df['WEIGHT'] = df['DOW'].apply(clipperWeights)
        df['LINKED_WEIGHT'] = df['WEIGHT'] / (1.0+df['LINKED_TRANSFERS'])
        
        # write it to an HDF file
        print(datetime.datetime.now(), '  write')
        key = 'm' + str(100*year + month) + '01'
        store = pd.HDFStore(outfile)
        store.append(key, df, data_columns=True)
        store.close()
    
        
