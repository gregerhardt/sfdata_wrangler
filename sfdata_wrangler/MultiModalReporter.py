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

import numpy as np
import pandas as pd
import datetime
from xlsxwriter.utility import xl_rowcol_to_cell

    
class MultiModalReporter():
    """ 
    Class to create drivers of demand reports and associated
    visuals and graphs. 
    """

    def __init__(self, multimodal_file, demand_file, ts_file, gtfs_file):
        '''
        Constructor. 

        '''   
        self.multimodal_file = multimodal_file
        self.demand_file = demand_file
        self.ts_file = ts_file
        self.gtfs_file = gtfs_file

        self.writer = None
        self.worksheet = None
        self.row = None
        self.col = None


    def assembleAnnualData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        mm_store = pd.HDFStore(self.multimodal_file)
        demand_store = pd.HDFStore(self.demand_file)
        
        transit = mm_store.select('transitAnnual')
        fares = mm_store.select('transitFareAnnual')
        acs = demand_store.select('countyACSannual')
        
        mm_store.close()
        demand_store.close()

        # start with the population, which has the longest time-series, 
        # and join all the others with the month being equivalent
        df = pd.merge(transit, fares, how='left', on=['FISCAL_YEAR'],  sort=True, suffixes=('', '_FARE')) 
        df = pd.merge(df, acs, how='left', left_on=['FISCAL_YEAR'], right_on=['YEAR'], sort=True, suffixes=('', '_ACS')) 

        return df


    def assembleMonthlyData(self):
        '''
        Calculates the fields used in the system performance reports
        and stores them in an HDF datastore. 
        '''   
        # open and join the input fields
        mm_store = pd.HDFStore(self.multimodal_file)
        demand_store = pd.HDFStore(self.demand_file)
        ts_store = pd.HDFStore(self.ts_file)
        gtfs_store = pd.HDFStore(self.gtfs_file)
        
        transit = mm_store.select('transitMonthly')
        fares = mm_store.select('transitFare')
        acs = demand_store.select('countyACS')
        bart = mm_store.select('bart_weekday', where="FROM='Entries' and TO='Exits'")
        
        # schedule data
        gtfs_bart     = gtfs_store.select('bartMonthly', where='DOW=1 and ROUTE_TYPE=1')
        gtfs_munibus  = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=3')
        gtfs_munirail = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=0')
        gtfs_municc   = gtfs_store.select('sfmuniMonthly', where='DOW=1 and ROUTE_TYPE=5')        
        
        # more specific data
        ts = ts_store.select('system_day_s', where='DOW=1') 
        muni = ts[['MONTH']].copy()
        muni['APC_ON_MUNI_BUS'] = ts['ON']
        bart['APC_ON_BART'] = bart['RIDERS']
        
        mm_store.close()
        demand_store.close()
        ts_store.close()
        gtfs_store.close()

        # join all the others with the month being equivalent
        df = pd.merge(transit, fares, how='left', on=['MONTH'],  sort=True, suffixes=('', '_FARE')) 
        df = pd.merge(df, acs, how='left', on=['MONTH'],  sort=True, suffixes=('', '_ACS')) 
        df = pd.merge(df, muni, how='left', on=['MONTH'],  sort=True, suffixes=('', '_MUNI')) 
        df = pd.merge(df, bart, how='left', on=['MONTH'],  sort=True, suffixes=('', '_BART')) 

        # do the first one twice so we get the suffixes right
        df = pd.merge(df, gtfs_bart, how='left', on=['MONTH'],  sort=True, suffixes=('', '_NOT_USED')) 
        df = pd.merge(df, gtfs_bart, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_BART')) 
        df = pd.merge(df, gtfs_munibus, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_BUS')) 
        df = pd.merge(df, gtfs_munirail, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_RAIL')) 
        df = pd.merge(df, gtfs_municc, how='left', on=['MONTH'],  sort=True, suffixes=('', '_GTFS_MUNI_CC')) 
        
        return df

        
    def writeMultiModalReport(self, xlsfile, comments=None):
        '''
        Writes a drivers of demand for all months to the specified excel file.        
        '''    
         
        # establish the writer        
        self.writer = pd.ExcelWriter(xlsfile, engine='xlsxwriter',
                        datetime_format='mmm-yyyy')        
        
        self.writeAnnualSheet(sheetName='Fiscal Year', comments=comments)
        self.writeMonthlySheet(sheetName='Monthly', comments=comments)
        
        self.writer.save()

    
    def writeAnnualSheet(self, sheetName, comments=None):         
                    
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]

        # get the actual data
        df = self.assembleAnnualData()    

        # Write the month as the column headers
        years = df[['FISCAL_YEAR']]
        years.T.to_excel(self.writer, sheet_name=sheetName,  
                                startrow=10, startcol=7, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})        
            
        # set the column widths
        worksheet.set_column(0, 0, 1)
        worksheet.set_column(1, 1, 3, bold)
        worksheet.set_column(2, 2, 35)
        worksheet.set_column(3, 3, 20)
        worksheet.set_column(4, 4, 15)
        worksheet.set_column(5, 5, 10)
        worksheet.set_column(6, 6, 25)    
        worksheet.set_column(7, 1000, 12)                
            
        # write the header
        worksheet.write(1, 1, 'San Francisco MultiModal Performance Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, 'San Francisco County')
        worksheet.write(5, 2, 'Temporal Resolution: ')
        worksheet.write(5, 3, 'Fiscal Year')
        worksheet.write(6, 2, 'Report Generated on: ')
        worksheet.write(6, 3, timestring)
        worksheet.write(7, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
            
        # Use formulas to calculate the differences
        self.set_position(self.writer, worksheet, 7, 2)  
        self.writeAnnualValues(df, years, sheetName, 'values')
        self.writeAnnualValues(df, years, sheetName, 'diff')
        self.writeAnnualValues(df, years, sheetName, 'pctDiff')
            
        # freeze so we can see what's happening
        worksheet.freeze_panes(0, 7)
            

    def writeAnnualValues(self, df, years, sheetName, formulaType='values'):
        '''
        Writes the main system values to the worksheet. 
        '''
                
        # leave a couple of empty spaces
        self.row += 2
        
        # which cells to look at
        ROW_OFFSET = self.row - 9
        COL_OFFSET = 1
        max_col = 6+len(years)+1

        # get the worksheet
        worksheet = self.writer.sheets[sheetName]      
        
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})
        int_format = self.writer.book.add_format({'num_format': '#,##0'})
        dec_format = self.writer.book.add_format({'num_format': '#,##0.00'})
        cent_format = self.writer.book.add_format({'num_format': '$#,##0.00'})
        dollar_format = self.writer.book.add_format({'num_format': '$#,##0'})
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        
        # HEADER
        if formulaType=='diff': 
            worksheet.write(self.row, 7, 'Difference from year before', bold)
        elif formulaType=='pctDiff': 
            worksheet.write(self.row, 7, 'Percent difference from year before', bold)
        else:        
            worksheet.write(self.row, 7, 'Values', bold)
        self.row += 1
        
        worksheet.write(self.row, 3, 'Source', bold)        
        worksheet.write(self.row, 4, 'Temporal Res', bold)        
        worksheet.write(self.row, 5, 'Geog Res', bold)        
        worksheet.write(self.row, 6, 'Trend', bold) 
        years.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=self.row, startcol=7, header=False, index=False) 
        self.row += 1       

        
        # TRANSIT STATISTICAL SUMMARY DATA
        measures = [('Annual Service Miles', 'SERVMILES', 'Transit Stat Summary', 'FY', int_format), 
                    ('Annual Ridership', 'PASSENGERS', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Weekday Ridership', 'AVG_WEEKDAY_RIDERSHIP', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Fare (2010$)', 'AVG_FARE_2010USD', 'Transit Stat Summary', 'FY', cent_format), 
                    ('Cash Fare (2010$)', 'CASH_FARE_2010USD', 'Published Values', 'Actual', cent_format), 
                    ]
        
        modes = [('Muni Bus+Rail', 'MUNI'), 
                 ('Muni Bus', 'MUNI_BUS'),
                 ('Muni Cable Car', 'MUNI_CC'),
                 ('Muni Rail', 'MUNI_RAIL'),
                 ('BART', 'BART'),
                 ('Caltrain', 'CALTRAIN')
                ]
                
        for header, measure, source, tempRes, format in measures: 
            worksheet.write(self.row, 1, header, bold)
            self.row += 1
                
            for label, mode in modes: 
                if measure + '_' + mode in df.columns: 

                    if formulaType=='values':
                        self.write_row(label=label, data=df[[measure + '_' + mode]], 
                            source=source, tempRes=tempRes, geogRes='System', format=format)

                    else: 
                        self.write_difference_row(label=label, 
                            row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                            source=source, tempRes=tempRes, geogRes='System', format=format, 
                            formulaType=formulaType)

        # MODE SHARES        
        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK',    'Walk'),
                 ('OTHER',   'Taxi, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares', bold)
        self.row += 1

        for mode, modeName in modes: 
            key = 'JTW_' + mode + '_SHARE'
            label =  modeName
            
            if formulaType=='values':
                self.write_row(label=label, data=df[[key]], 
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
            else: 
                self.write_difference_row(label=label, 
                    row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                            formulaType=formulaType)


        # MODE SHARES BY SEGMENT   
        groups = [('JTW_EARN0_50_',    'Workers earning $0-50k: '),
                  ('JTW_EARN50P_',     'Workers earning $50k+: '),
                  ('JTW_0VEH_',        'Workers with 0 vehicles: '),
                  ('JTW_1PVEH_',       'Workers with 1+ vehicles: ')]

        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK_OTHER','Taxi, walk, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares by Segment', bold)
        self.row += 1

        for group, groupName in groups: 
            for mode, modeName in modes: 

                key = group + mode + '_SHARE'
                label = groupName + modeName

                if formulaType=='values':         
                    self.write_row(label=label, data=df[[key]], 
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
                else: 
                    self.write_difference_row(label=label, 
                        row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                                formulaType=formulaType)


    def writeMonthlySheet(self, sheetName, comments=None):         
                    
        timestring = str(pd.Timestamp(datetime.datetime.now()))
        timestring = timestring.split('.')[0]

        # get the actual data
        df = self.assembleMonthlyData()    

        # Write the month as the column headers
        periods = df[['MONTH']]
        periods.T.to_excel(self.writer, sheet_name=sheetName,  
                                startrow=10, startcol=7, header=False, index=False)
                    
    
        # note that we have to call the pandas function first to get the
        # excel sheet created properly, so now we can access that
        worksheet = self.writer.sheets[sheetName]
            
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})        
            
        # set the column widths
        worksheet.set_column(0, 0, 1)
        worksheet.set_column(1, 1, 3, bold)
        worksheet.set_column(2, 2, 35)
        worksheet.set_column(3, 3, 20)
        worksheet.set_column(4, 4, 15)
        worksheet.set_column(5, 5, 10)
        worksheet.set_column(6, 6, 25)    
        worksheet.set_column(7, 1000, 12)                
            
        # write the header
        worksheet.write(1, 1, 'San Francisco MultiModal Performance Report', bold)
        worksheet.write(3, 1, 'Input Specification', bold)
        worksheet.write(4, 2, 'Geographic Extent: ')
        worksheet.write(4, 3, 'San Francisco County')
        worksheet.write(5, 2, 'Temporal Resolution: ')
        worksheet.write(5, 3, 'Month')
        worksheet.write(6, 2, 'Report Generated on: ')
        worksheet.write(6, 3, timestring)
        worksheet.write(7, 2, 'Comments: ')      
        worksheet.write(8, 3, comments)        
            
        # Use formulas to calculate the differences
        self.set_position(self.writer, worksheet, 7, 2)  
        self.writeMonthlyValues(df, periods, sheetName, 'values')
        self.writeMonthlyValues(df, periods, sheetName, 'diff')
        self.writeMonthlyValues(df, periods, sheetName, 'pctDiff')
                        
        # freeze so we can see what's happening
        worksheet.freeze_panes(0, 7)


    def writeMonthlyValues(self, df, months, sheetName, formulaType='values'):
        '''
        Writes the main system values to the worksheet. 
        '''
                
        # leave a couple of empty spaces
        self.row += 2
        
        # which cells to look at
        ROW_OFFSET = self.row - 9
        COL_OFFSET = 12
        max_col = 6+len(months)+1

        # get the worksheet
        worksheet = self.writer.sheets[sheetName]        
        
        # set up the formatting, with defaults
        bold = self.writer.book.add_format({'bold': 1})
        int_format = self.writer.book.add_format({'num_format': '#,##0'})
        dec_format = self.writer.book.add_format({'num_format': '#,##0.00'})
        cent_format = self.writer.book.add_format({'num_format': '$#,##0.00'})
        dollar_format = self.writer.book.add_format({'num_format': '$#,##0'})
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        
        
        # HEADER
        if formulaType=='diff': 
            worksheet.write(self.row, 7, 'Difference from year before', bold)
        elif formulaType=='pctDiff': 
            worksheet.write(self.row, 7, 'Percent difference from year before', bold)
        else:        
            worksheet.write(self.row, 7, 'Values', bold)
        self.row += 1
        
        worksheet.write(self.row, 3, 'Source', bold)        
        worksheet.write(self.row, 4, 'Temporal Res', bold)        
        worksheet.write(self.row, 5, 'Geog Res', bold)        
        worksheet.write(self.row, 6, 'Trend', bold) 
        months.T.to_excel(self.writer, sheet_name=sheetName, 
                            startrow=self.row, startcol=7, header=False, index=False) 
        self.row += 1       

        
        # TRANSIT STATISTICAL SUMMARY DATA
        measures = [('Monthly Service Miles', 'SERVMILES', 'Transit Stat Summary', 'FY', int_format),
                    ('Monthly Ridership', 'PASSENGERS', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Weekday Ridership', 'AVG_WEEKDAY_RIDERSHIP', 'Transit Stat Summary', 'FY', int_format), 
                    ('Average Weekday Ridership', 'APC_ON', 'APCs/Faregate', 'Monthly', int_format), 
                    ('Average Fare (2010$)', 'AVG_FARE_2010USD', 'Transit Stat Summary', 'FY/Actual', cent_format), 
                    ('Cash Fare (nominal $)', 'FARE_GTFS', 'GTFS', 'Actual', cent_format),  
                    ('Weekday Service Miles', 'SERVMILES_S_GTFS', 'GTFS', 'Actual', int_format),  
                    ('Weekday Average Headway', 'HEADWAY_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ('Weekday Average Run Speed', 'RUNSPEED_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ('Weekday Average Total Speed', 'TOTSPEED_S_GTFS', 'GTFS', 'Actual', dec_format),  
                    ]
        
        
        modes = [('Muni Bus+Rail', 'MUNI'), 
                 ('Muni Bus', 'MUNI_BUS'),
                 ('Muni Cable Car', 'MUNI_CC'),
                 ('Muni Rail', 'MUNI_RAIL'),
                 ('BART', 'BART'),
                 ('Caltrain', 'CALTRAIN')
                ]
                        
        for header, measure, source, tempRes, format in measures: 
            worksheet.write(self.row, 1, header, bold)
            self.row += 1
                
            for label, mode in modes: 
                if measure + '_' + mode in df.columns:                     
                    if formulaType=='values':
                        self.write_row(label=label, data=df[[measure + '_' + mode]], 
                            source=source, tempRes=tempRes, geogRes='System', format=format)

                    else: 
                        self.write_difference_row(label=label, 
                            row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                            source=source, tempRes=tempRes, geogRes='System', format=format, 
                            formulaType=formulaType)

                else:
                    print "can't find ",  measure + '_' + mode

        # MODE SHARES        
        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK',    'Walk'),
                 ('OTHER',   'Taxi, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares', bold)
        self.row += 1

        for mode, modeName in modes: 
            key = 'JTW_' + mode + '_SHARE'
            label =  modeName
            
            if formulaType=='values':
                self.write_row(label=label, data=df[[key]], 
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
            else: 
                self.write_difference_row(label=label, 
                    row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                    source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                            formulaType=formulaType)


        # MODE SHARES BY SEGMENT   
        groups = [('JTW_EARN0_50_',    'Workers earning $0-50k: '),
                  ('JTW_EARN50P_',     'Workers earning $50k+: '),
                  ('JTW_0VEH_',        'Workers with 0 vehicles: '),
                  ('JTW_1PVEH_',       'Workers with 1+ vehicles: ')]

        modes = [('DA',      'Drive-Alone'), 
                 ('SR',      'Carpool'),
                 ('TRANSIT', 'Transit'),
                 ('WALK_OTHER','Taxi, walk, bike, other'),
                 ('HOME',    'Work at home')]

        worksheet.write(self.row, 1, 'Commute Mode Shares by Segment', bold)
        self.row += 1

        for group, groupName in groups: 
            for mode, modeName in modes: 

                key = group + mode + '_SHARE'
                label = groupName + modeName

                if formulaType=='values':
                    self.write_row(label=label, data=df[[key]], 
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format)
                else: 
                    self.write_difference_row(label=label, 
                        row_offset=ROW_OFFSET, col_offset=COL_OFFSET, max_col=max_col,
                        source='ACS', tempRes='Annual', geogRes='County', format=percent_format, 
                                formulaType=formulaType)


    def set_position(self, writer, worksheet, row, col):
        '''
        sets the position where the next item will be written. 
        
        '''

        self.writer = writer
        self.worksheet = worksheet
        self.row = row
        self.col = col
        
        
                                    
    def write_row(self, label, source, tempRes, geogRes, data, format, sparkline=True):
        '''
        Writes a row of data to the worksheet.
        
        worksheet - where to write it
        r - start row
        c - start column
        label - string label to write in first column
        source - string source to write in second column
        res - string resolution to write in third column
        format - number format for the row
        sparkline - boolean indicating whether or not to add a sparkline
        
        '''
        
        # labels
        self.worksheet.write(self.row, self.col, label)
        self.worksheet.write(self.row, self.col+1, source)
        self.worksheet.write(self.row, self.col+2, tempRes)
        self.worksheet.write(self.row, self.col+3, geogRes)

        # data
        self.worksheet.set_row(self.row, None, format) 
        data.T.to_excel(self.writer, sheet_name=self.worksheet.get_name(), 
                            startrow=self.row, startcol=self.col+5, header=False, index=False)
        
        # sparkline
        if sparkline: 
            cell = xl_rowcol_to_cell(self.row, self.col+4)
            data_range = (xl_rowcol_to_cell(self.row, self.col+5) + 
                   ':' + xl_rowcol_to_cell(self.row, self.col+5+len(data)+1))
            self.worksheet.add_sparkline(cell, {'range': data_range})   

        # increment the row
        self.row += 1


                                    
    def write_difference_row(self, row_offset, col_offset, max_col, 
        label, source, tempRes, geogRes, format, sparkline=True, 
        formulaType='diff'):
        '''
        Writes a row of difference formulas to the worksheet.
        
        worksheet - where to write it
        row_offset - offset for row formulas
        col_offset - offset for col formulas
        max_col - right most column
        label - string label to write in first column
        source - string source to write in second column
        res - string resolution to write in third column
        format - number format for the row
        sparkline - boolean indicating whether or not to add a sparkline
        formulaType - either 'diff' or 'pctDiff'
        '''
        
        # labels
        self.worksheet.write(self.row, self.col, label)
        self.worksheet.write(self.row, self.col+1, source)
        self.worksheet.write(self.row, self.col+2, tempRes)
        self.worksheet.write(self.row, self.col+3, geogRes)

        # formats
        percent_format = self.writer.book.add_format({'num_format': '0.0%'})
        if formulaType=='diff': 
            self.worksheet.set_row(self.row, None, format) 
        elif formulaType=='pctDiff': 
            self.worksheet.set_row(self.row, None, percent_format)             
        
        # formulas
        for c in range(7+col_offset, max_col):
            cell = xl_rowcol_to_cell(self.row, c)
            new = xl_rowcol_to_cell(self.row-row_offset, c)
            old = xl_rowcol_to_cell(self.row-row_offset, c-col_offset)
            if formulaType=='diff': 
                self.worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'-'+old+',"")')
            elif formulaType=='pctDiff':                 
                self.worksheet.write_formula(cell, '=IF(AND(ISNUMBER('+old+'),ISNUMBER('+new+')),'+new+'/'+old+'-1,"")')
            
        # sparkline
        if sparkline: 
            data_range = xl_rowcol_to_cell(self.row, 7) + ':' + xl_rowcol_to_cell(self.row, max_col)
            self.worksheet.add_sparkline(self.row, 6, {'range': data_range, 
                                                       'type': 'column', 
                                                       'negative_points': True})                  
        # increment the row
        self.row += 1
        