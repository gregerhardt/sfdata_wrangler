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
import glob
import os


def convertToDate(dateString):
    '''
    Converts a string to a date.  
    '''
    date = pd.to_datetime(dateString)
    if date < pd.to_datetime('1990-01-01'): 
        date = pd.NaT
    return date


def convertDateToMonth(date):
    '''
    Given a date, returns the month
    '''
    if pd.isnull(date): 
        return pd.NaT
    else: 
        month = ((pd.to_datetime(date)).to_period('M')).to_timestamp() 
        return month

    
class DemandHelper():
    """ 
    Class to create drivers of demand data: employment, population, fuel cost.
    
    """

    # the range of years for these data files
    POP_EST_YEARS = [2000,2014]
    HU_YEARS      = [2001,2014]
    ACS_YEARS     = [2005,2014]
    LODES_YEARS   = [2002,2014]  
    
    # a list of output field and inputfield tuples for each table
    ACS_EQUIV = {'B01003' : [('POP',   'Estimate; Total')
                            ], 
                 'DP04' :   [('UNITS_ACS', 'Estimate; HOUSING OCCUPANCY - Total housing units')
                            ], 
                 'DP03'   : [('HH',           'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Total households'),
                             ('WORKERS',      'Estimate; EMPLOYMENT STATUS - In labor force - Civilian labor force - Employed'), 
                             ('MEDIAN_HHINC', 'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Median household income (dollars)'),
                             ('HH_INC0_15',  ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - Less than $10,000',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $10,000 to $14,999']), 
                             ('HH_INC15_50', ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $15,000 to $24,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $25,000 to $34,999', 
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $35,000 to $49,999']), 
                             ('HH_INC50_100',['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $50,000 to $74,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $75,000 to $99,999']), 
                             ('HH_INC100P',  ['Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $100,000 to $149,999',
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $150,000 to $199,999', 
                                              'Estimate; INCOME AND BENEFITS (IN YYYY INFLATION-ADJUSTED DOLLARS) - $200,000 or more'])
                            ], 
                 'B08203' : [('HH_0VEH',      'Estimate; No vehicle available'),
                             ('HH_1VEH',      'Estimate; 1 vehicle available'), 
                             ('HH_2PVEH',    ['Estimate; 2 vehicles available',
                                              'Estimate; 3 vehicles available', 
                                              'Estimate; 4 vehicles available'])
                            ],
                            
                            # total mode shares
                 'B08119' : [('JTW_DA',       'Estimate; Car, truck, or van - drove alone:'),
                             ('JTW_SR',       'Estimate; Car, truck, or van - carpooled:'), 
                             ('JTW_TRANSIT',  'Estimate; Public transportation (excluding taxicab):'), 
                             ('JTW_WALK',     'Estimate; Walked:'), 
                             ('JTW_OTHER',    'Estimate; Taxicab, motorcycle, bicycle, or other means:'), 
                             ('JTW_HOME',     'Estimate; Worked at home:'),                              
                             
                             # workers earnings $0-50k vs $50k+
                             ('JTW_EARN0_50_DA',      ['Estimate; Car, truck, or van - drove alone: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - drove alone: - $10,000 to $14,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $35,000 to $49,999']),
                             ('JTW_EARN0_50_SR',      ['Estimate; Car, truck, or van - carpooled: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - carpooled: - $10,000 to $14,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $35,000 to $49,999']),
                             ('JTW_EARN0_50_TRANSIT', ['Estimate; Public transportation (excluding taxicab): - $1 to $9,999 or loss', 
                                                       'Estimate; Public transportation (excluding taxicab): - $10,000 to $14,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $15,000 to $24,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $25,000 to $34,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $35,000 to $49,999']),
                             ('JTW_EARN0_50_WALK_OTHER',['Estimate; Walked: - $1 to $9,999 or loss', 
                                                       'Estimate; Walked: - $10,000 to $14,999',
                                                       'Estimate; Walked: - $15,000 to $24,999', 
                                                       'Estimate; Walked: - $25,000 to $34,999',
                                                       'Estimate; Walked: - $35,000 to $49,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $1 to $9,999 or loss', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $10,000 to $14,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $15,000 to $24,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $25,000 to $34,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $35,000 to $49,999']),
                             ('JTW_EARN0_50_HOME',    ['Estimate; Worked at home: - $1 to $9,999 or loss', 
                                                       'Estimate; Worked at home: - $10,000 to $14,999',
                                                       'Estimate; Worked at home: - $15,000 to $24,999', 
                                                       'Estimate; Worked at home: - $25,000 to $34,999',
                                                       'Estimate; Worked at home: - $35,000 to $49,999']), 
                             
                             ('JTW_EARN50P_DA',       ['Estimate; Car, truck, or van - drove alone: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $65,000 to $74,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $75,000 or more']),
                             ('JTW_EARN50P_SR',       ['Estimate; Car, truck, or van - carpooled: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $65,000 to $74,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $75,000 or more']),
                             ('JTW_EARN50P_TRANSIT',  ['Estimate; Public transportation (excluding taxicab): - $50,000 to $64,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $65,000 to $74,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $75,000 or more']),
                             ('JTW_EARN50P_WALK_OTHER',['Estimate; Walked: - $50,000 to $64,999', 
                                                       'Estimate; Walked: - $65,000 to $74,999',
                                                       'Estimate; Walked: - $75,000 or more', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $50,000 to $64,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $65,000 to $74,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $75,000 or more']),
                             ('JTW_EARN50P_HOME',     ['Estimate; Worked at home: - $50,000 to $64,999', 
                                                       'Estimate; Worked at home: - $65,000 to $74,999',
                                                       'Estimate; Worked at home: - $75,000 or more']), 
                                                       
                             # workers earning $0-75k vs 75k+
                             ('JTW_EARN0_75_DA',      ['Estimate; Car, truck, or van - drove alone: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - drove alone: - $10,000 to $14,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - drove alone: - $35,000 to $49,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - drove alone: - $65,000 to $74,999']),
                             ('JTW_EARN0_75_SR',      ['Estimate; Car, truck, or van - carpooled: - $1 to $9,999 or loss', 
                                                       'Estimate; Car, truck, or van - carpooled: - $10,000 to $14,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $15,000 to $24,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $25,000 to $34,999',
                                                       'Estimate; Car, truck, or van - carpooled: - $35,000 to $49,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $50,000 to $64,999', 
                                                       'Estimate; Car, truck, or van - carpooled: - $65,000 to $74,999']),
                             ('JTW_EARN0_75_TRANSIT', ['Estimate; Public transportation (excluding taxicab): - $1 to $9,999 or loss', 
                                                       'Estimate; Public transportation (excluding taxicab): - $10,000 to $14,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $15,000 to $24,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $25,000 to $34,999',
                                                       'Estimate; Public transportation (excluding taxicab): - $35,000 to $49,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $50,000 to $64,999', 
                                                       'Estimate; Public transportation (excluding taxicab): - $65,000 to $74,999']),
                             ('JTW_EARN0_75_WALK_OTHER',['Estimate; Walked: - $1 to $9,999 or loss', 
                                                       'Estimate; Walked: - $10,000 to $14,999',
                                                       'Estimate; Walked: - $15,000 to $24,999', 
                                                       'Estimate; Walked: - $25,000 to $34,999',
                                                       'Estimate; Walked: - $35,000 to $49,999', 
                                                       'Estimate; Walked: - $50,000 to $64,999', 
                                                       'Estimate; Walked: - $65,000 to $74,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $1 to $9,999 or loss', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $10,000 to $14,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $15,000 to $24,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $25,000 to $34,999',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $35,000 to $49,999',                                                        
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $50,000 to $64,999', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - $65,000 to $74,999']),
                             ('JTW_EARN0_75_HOME',    ['Estimate; Worked at home: - $1 to $9,999 or loss', 
                                                       'Estimate; Worked at home: - $10,000 to $14,999',
                                                       'Estimate; Worked at home: - $15,000 to $24,999', 
                                                       'Estimate; Worked at home: - $25,000 to $34,999',
                                                       'Estimate; Worked at home: - $35,000 to $49,999', 
                                                       'Estimate; Worked at home: - $50,000 to $64,999', 
                                                       'Estimate; Worked at home: - $65,000 to $74,999']), 
                             
                             ('JTW_EARN75P_DA',       ['Estimate; Car, truck, or van - drove alone: - $75,000 or more']),
                             ('JTW_EARN75P_SR',       ['Estimate; Car, truck, or van - carpooled: - $75,000 or more']),
                             ('JTW_EARN75P_TRANSIT',  ['Estimate; Public transportation (excluding taxicab): - $75,000 or more']),
                             ('JTW_EARN75P_WALK_OTHER',['Estimate; Walked: - $75,000 or more',
                                                        'Estimate; Taxicab, motorcycle, bicycle, or other means: - $75,000 or more']),
                             ('JTW_EARN75P_HOME',     ['Estimate; Worked at home: - $75,000 or more'])
                            ],
                            
                 'B08141' : [('JTW_0VEH_DA',           'Estimate; Car, truck, or van - drove alone: - No vehicle available'),
                             ('JTW_0VEH_SR',           'Estimate; Car, truck, or van - carpooled: - No vehicle available'),
                             ('JTW_0VEH_TRANSIT',      'Estimate; Public transportation (excluding taxicab): - No vehicle available'),
                             ('JTW_0VEH_WALK_OTHER',  ['Estimate; Walked: - No vehicle available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - No vehicle available']),
                             ('JTW_0VEH_HOME',         'Estimate; Worked at home: - No vehicle available'),
                                                          
                             ('JTW_1PVEH_DA',         ['Estimate; Car, truck, or van - drove alone: - 1 vehicle available', 
                                                       'Estimate; Car, truck, or van - drove alone: - 2 vehicles available', 
                                                       'Estimate; Car, truck, or van - drove alone: - 3 vehicles available',
                                                       'Estimate; Car, truck, or van - drove alone: - 4 vehicles available',
                                                       'Estimate; Car, truck, or van - drove alone: - 5 vehicles available']),
                             ('JTW_1PVEH_SR',         ['Estimate; Car, truck, or van - carpooled: - 1 vehicle available', 
                                                       'Estimate; Car, truck, or van - carpooled: - 2 vehicles available', 
                                                       'Estimate; Car, truck, or van - carpooled: - 3 vehicles available',
                                                       'Estimate; Car, truck, or van - carpooled: - 4 vehicles available',
                                                       'Estimate; Car, truck, or van - carpooled: - 5 vehicles available']),
                             ('JTW_1PVEH_TRANSIT',    ['Estimate; Public transportation (excluding taxicab): - 1 vehicle available', 
                                                       'Estimate; Public transportation (excluding taxicab): - 2 vehicles available', 
                                                       'Estimate; Public transportation (excluding taxicab): - 3 vehicles available',
                                                       'Estimate; Public transportation (excluding taxicab): - 4 vehicles available',
                                                       'Estimate; Public transportation (excluding taxicab): - 5 vehicles available']),
                             ('JTW_1PVEH_WALK_OTHER', ['Estimate; Walked: - 1 vehicle available', 
                                                       'Estimate; Walked: - 2 vehicles available', 
                                                       'Estimate; Walked: - 3 vehicles available',
                                                       'Estimate; Walked: - 4 vehicles available',
                                                       'Estimate; Walked: - 5 vehicles available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 1 vehicle available', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 2 vehicles available', 
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 3 vehicles available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 4 vehicles available',
                                                       'Estimate; Taxicab, motorcycle, bicycle, or other means: - 5 vehicles available']),
                             ('JTW_1PVEH_HOME',       ['Estimate; Worked at home: - 1 vehicle available', 
                                                       'Estimate; Worked at home: - 2 vehicles available', 
                                                       'Estimate; Worked at home: - 3 vehicles available',
                                                       'Estimate; Worked at home: - 4 vehicles available',
                                                       'Estimate; Worked at home: - 5 vehicles available']),
                            ], 
                            
                 # population by age and gender
                 'S0101'  : [('MALE_PCT0_19',    ['Male; Estimate; AGE - Under 5 years',
                                              'Male; Estimate; AGE - 5 to 9 years',
                                              'Male; Estimate; AGE - 10 to 14 years',
                                              'Male; Estimate; AGE - 15 to 19 years']),                                              
                             ('MALE_PCT20_29',   ['Male; Estimate; AGE - 20 to 24 years',
                                              'Male; Estimate; AGE - 25 to 29 years']),                                              
                             ('MALE_PCT30_64',   ['Male; Estimate; AGE - 30 to 34 years',
                                              'Male; Estimate; AGE - 35 to 39 years',
                                              'Male; Estimate; AGE - 40 to 44 years',
                                              'Male; Estimate; AGE - 45 to 49 years',
                                              'Male; Estimate; AGE - 50 to 54 years',
                                              'Male; Estimate; AGE - 55 to 59 years',
                                              'Male; Estimate; AGE - 60 to 64 years']),                             
                             ('MALE_PCT65P',      'Male; Estimate; SELECTED AGE CATEGORIES - 65 years and over'), 

                             ('FEMALE_PCT0_19',  ['Female; Estimate; AGE - Under 5 years',
                                              'Female; Estimate; AGE - 5 to 9 years',
                                              'Female; Estimate; AGE - 10 to 14 years',
                                              'Female; Estimate; AGE - 15 to 19 years']),                                              
                             ('FEMALE_PCT20_29', ['Female; Estimate; AGE - 20 to 24 years',
                                              'Female; Estimate; AGE - 25 to 29 years']),                                              
                             ('FEMALE_PCT30_64', ['Female; Estimate; AGE - 30 to 34 years',
                                              'Female; Estimate; AGE - 35 to 39 years',
                                              'Female; Estimate; AGE - 40 to 44 years',
                                              'Female; Estimate; AGE - 45 to 49 years',
                                              'Female; Estimate; AGE - 50 to 54 years',
                                              'Female; Estimate; AGE - 55 to 59 years',
                                              'Female; Estimate; AGE - 60 to 64 years']),                             
                             ('FEMALE_PCT65P',    'Female; Estimate; SELECTED AGE CATEGORIES - 65 years and over'), 
                             
                             ('MALE',         'Male; Estimate; Total population'),
                             ('FEMALE',       'Female; Estimate; Total population'),
                             ('MEDIAN_AGE',   'Total; Estimate; SUMMARY INDICATORS - Median age (years)')
                            ]
                            
                }

    
    
    # a list of output field and inputfield tuples for each table  in Census 2000
    CENSUS2000_EQUIV = {
                 'DP01' :  [('POP',          'Number; Total population'), 
                            ('HH',           'Number; HOUSEHOLDS BY TYPE - Total households'), 
                            ('UNITS_ACS',    'Number; HOUSING OCCUPANCY - Total housing units'),                              
                            ('MEDIAN_AGE',   'Number; Total population - SEX AND AGE - Median age (years)')
                            ], 
                 'DP03'   : [('WORKERS',      'Number; EMPLOYMENT STATUS - Population 16 years and over - In labor force - Civilian labor force - Employed'), 
                             ('MEDIAN_HHINC', 'Number; INCOME IN 1999 - Families - Median family income (dollars)'), 
                             ('HH_INC0_15',  ['Number; INCOME IN 1999 - Households - Less than $10,000',
                                              'Number; INCOME IN 1999 - Households - $10,000 to $14,999']), 
                             ('HH_INC15_50', ['Number; INCOME IN 1999 - Households - $15,000 to $24,999',
                                              'Number; INCOME IN 1999 - Households - $25,000 to $34,999', 
                                              'Number; INCOME IN 1999 - Households - $35,000 to $49,999']), 
                             ('HH_INC50_100',['Number; INCOME IN 1999 - Households - $50,000 to $74,999',
                                              'Number; INCOME IN 1999 - Households - $75,000 to $99,999']), 
                             ('HH_INC100P',  ['Number; INCOME IN 1999 - Households - $100,000 to $149,999',
                                              'Number; INCOME IN 1999 - Households - $150,000 to $199,999', 
                                              'Number; INCOME IN 1999 - Households - $200,000 or more'])
                            ], 
                 '1-065' :  [('HH_0VEH',      'TAB65X2'),
                             ('HH_1VEH',      'TAB65X3'), 
                             ('HH_2PVEH',    ['TAB65X4','TAB65X5','TAB65X6'])
                            ],
                 'P030' :   [('JTW_DA',       'Car, truck, or van: - Drove alone'),
                             ('JTW_SR',       'Car, truck, or van: - Carpooled'), 
                             ('JTW_TRANSIT',  ['Public transportation: - Bus or trolley bus', 
                                               'Public transportation: - Streetcar or trolley car (publico in Puerto Rico)', 
                                               'Public transportation: - Subway or elevated', 
                                               'Public transportation: - Railroad', 
                                               'Public transportation: - Ferryboat']), 
                             ('JTW_WALK',     'Walked'), 
                             ('JTW_OTHER',   ['Public transportation: - Taxicab', 
                                              'Motorcycle', 
                                              'Bicycle', 
                                              'Other means']), 
                             ('JTW_HOME',     'Worked at home')
                            ],            
                            # mode by $0-50k vs $50k+
                 '1-013' :  [('JTW_EARN0_50_DA',      ['TAB13X13', 
                                                       'TAB13X23', 
                                                       'TAB13X35', 
                                                       'TAB13X46', 
                                                       'TAB13X57', 
                                                       'TAB13X68',
                                                       'TAB13X79',
                                                       'TAB13X90']),
                             ('JTW_EARN0_50_SR',      ['TAB13X14', 'TAB13X15', 'TAB13X16', 
                                                       'TAB13X24', 'TAB13X25', 'TAB13X26',
                                                       'TAB13X36', 'TAB13X37', 'TAB13X38', 
                                                       'TAB13X47', 'TAB13X48', 'TAB13X49', 
                                                       'TAB13X58', 'TAB13X59', 'TAB13X60', 
                                                       'TAB13X69', 'TAB13X70', 'TAB13X71', 
                                                       'TAB13X80', 'TAB13X81', 'TAB13X82', 
                                                       'TAB13X91', 'TAB13X92', 'TAB13X93']),
                             ('JTW_EARN0_50_TRANSIT', ['TAB13X17', 'TAB13X18', 'TAB13X19', 
                                                       'TAB13X27', 'TAB13X28', 'TAB13X29',
                                                       'TAB13X39', 'TAB13X40', 'TAB13X41', 
                                                       'TAB13X50', 'TAB13X51', 'TAB13X52', 
                                                       'TAB13X61', 'TAB13X62', 'TAB13X63', 
                                                       'TAB13X72', 'TAB13X73', 'TAB13X74', 
                                                       'TAB13X83', 'TAB13X84', 'TAB13X85', 
                                                       'TAB13X94', 'TAB13X95', 'TAB13X96']),
                             ('JTW_EARN0_50_WALK_OTHER',['TAB13X20', 'TAB13X21',  
                                                       'TAB13X30', 'TAB13X31', 
                                                       'TAB13X42', 'TAB13X43', 
                                                       'TAB13X53', 'TAB13X54', 
                                                       'TAB13X64', 'TAB13X65', 
                                                       'TAB13X75', 'TAB13X76', 
                                                       'TAB13X86', 'TAB13X87', 
                                                       'TAB13X97', 'TAB13X98']),
                             ('JTW_EARN0_50_HOME',    ['TAB13X22', 
                                                       'TAB13X33', 
                                                       'TAB13X44', 
                                                       'TAB13X55', 
                                                       'TAB13X66', 
                                                       'TAB13X77', 
                                                       'TAB13X88', 
                                                       'TAB13X99']),
                             
                             ('JTW_EARN50P_DA',       ['TAB13X101', 
                                                       'TAB13X112']),
                             ('JTW_EARN50P_SR',       ['TAB13X102','TAB13X103','TAB13X104',
                                                       'TAB13X113','TAB13X114','TAB13X115']),
                             ('JTW_EARN50P_TRANSIT',  ['TAB13X105','TAB13X106','TAB13X107',
                                                       'TAB13X116','TAB13X117','TAB13X118']),
                             ('JTW_EARN50P_WALK_OTHER',['TAB13X108', 'TAB13X109',  
                                                       'TAB13X119', 'TAB13X120']),
                             ('JTW_EARN50P_HOME',     ['TAB13X110', 
                                                       'TAB13X121']),

                            # mode by $0-75k vs $75k+
                            ('JTW_EARN0_75_DA',      ['TAB13X13', 
                                                       'TAB13X23', 
                                                       'TAB13X35', 
                                                       'TAB13X46', 
                                                       'TAB13X57', 
                                                       'TAB13X68',
                                                       'TAB13X79',
                                                       'TAB13X90', 
                                                       'TAB13X101']),
                             ('JTW_EARN0_75_SR',      ['TAB13X14', 'TAB13X15', 'TAB13X16', 
                                                       'TAB13X24', 'TAB13X25', 'TAB13X26',
                                                       'TAB13X36', 'TAB13X37', 'TAB13X38', 
                                                       'TAB13X47', 'TAB13X48', 'TAB13X49', 
                                                       'TAB13X58', 'TAB13X59', 'TAB13X60', 
                                                       'TAB13X69', 'TAB13X70', 'TAB13X71', 
                                                       'TAB13X80', 'TAB13X81', 'TAB13X82', 
                                                       'TAB13X91', 'TAB13X92', 'TAB13X93', 
                                                       'TAB13X102','TAB13X103','TAB13X104']),
                             ('JTW_EARN0_75_TRANSIT', ['TAB13X17', 'TAB13X18', 'TAB13X19', 
                                                       'TAB13X27', 'TAB13X28', 'TAB13X29',
                                                       'TAB13X39', 'TAB13X40', 'TAB13X41', 
                                                       'TAB13X50', 'TAB13X51', 'TAB13X52', 
                                                       'TAB13X61', 'TAB13X62', 'TAB13X63', 
                                                       'TAB13X72', 'TAB13X73', 'TAB13X74', 
                                                       'TAB13X83', 'TAB13X84', 'TAB13X85', 
                                                       'TAB13X94', 'TAB13X95', 'TAB13X96', 
                                                       'TAB13X105','TAB13X106','TAB13X107']),
                             ('JTW_EARN0_75_WALK_OTHER',['TAB13X20', 'TAB13X21',  
                                                       'TAB13X30', 'TAB13X31', 
                                                       'TAB13X42', 'TAB13X43', 
                                                       'TAB13X53', 'TAB13X54', 
                                                       'TAB13X64', 'TAB13X65', 
                                                       'TAB13X75', 'TAB13X76', 
                                                       'TAB13X86', 'TAB13X87', 
                                                       'TAB13X97', 'TAB13X98', 
                                                       'TAB13X108', 'TAB13X109']),
                             ('JTW_EARN0_75_HOME',    ['TAB13X22', 
                                                       'TAB13X33', 
                                                       'TAB13X44', 
                                                       'TAB13X55', 
                                                       'TAB13X66', 
                                                       'TAB13X77', 
                                                       'TAB13X88', 
                                                       'TAB13X99', 
                                                       'TAB13X110']),
                             
                             ('JTW_EARN75P_DA',       ['TAB13X112']),
                             ('JTW_EARN75P_SR',       ['TAB13X113','TAB13X114','TAB13X115']),
                             ('JTW_EARN75P_TRANSIT',  ['TAB13X116','TAB13X117','TAB13X118']),
                             ('JTW_EARN75P_WALK_OTHER',['TAB13X119', 'TAB13X120']),
                             ('JTW_EARN75P_HOME',     ['TAB13X121'])

                            ],
                 '1-035' :  [('JTW_0VEH_DA',           'TAB35X13'),
                             ('JTW_0VEH_SR',           ['TAB35X14', 'TAB35X15', 'TAB35X16']),
                             ('JTW_0VEH_TRANSIT',      ['TAB35X17', 'TAB35X18', 'TAB35X19']),
                             ('JTW_0VEH_WALK_OTHER',   ['TAB35X20', 'TAB35X21']),
                             ('JTW_0VEH_HOME',         'TAB35X22'),
                             
                             ('JTW_1PVEH_DA',         ['TAB35X24', 
                                                       'TAB35X35',
                                                       'TAB35X46',
                                                       'TAB35X57']),
                             ('JTW_1PVEH_SR',         ['TAB35X25', 'TAB35X26', 'TAB35X27',
                                                       'TAB35X36', 'TAB35X37', 'TAB35X38',
                                                       'TAB35X47', 'TAB35X48', 'TAB35X49',
                                                       'TAB35X58', 'TAB35X59', 'TAB35X60']),
                             ('JTW_1PVEH_TRANSIT',    ['TAB35X28', 'TAB35X29', 'TAB35X30',
                                                       'TAB35X39', 'TAB35X40', 'TAB35X41',
                                                       'TAB35X50', 'TAB35X51', 'TAB35X52',
                                                       'TAB35X61', 'TAB35X62', 'TAB35X63']),
                             ('JTW_1PVEH_WALK_OTHER', ['TAB35X31', 'TAB35X32',
                                                       'TAB35X42', 'TAB35X43', 
                                                       'TAB35X53', 'TAB35X54', 
                                                       'TAB35X64', 'TAB35X65']),
                             ('JTW_1PVEH_HOME',       ['TAB35X33', 
                                                       'TAB35X44',
                                                       'TAB35X55',
                                                       'TAB35X66'])
                            ], 
                            
                 # population by age and gender
                 'SF1_P012':[('MALE0_19',   ['Male: - Under 5 years',
                                              'Male: - 5 to 9 years',
                                              'Male: - 10 to 14 years',
                                              'Male: - 15 to 17 years',
                                              'Male: - 18 and 19 years']),                                              
                             ('MALE20_29',   ['Male: - 20 years',
                                              'Male: - 21 years',
                                              'Male: - 22 to 24 years',
                                              'Male: - 25 to 29 years']),                                              
                             ('MALE30_64',   ['Male: - 30 to 34 years',
                                              'Male: - 35 to 39 years',
                                              'Male: - 40 to 44 years',
                                              'Male: - 45 to 49 years',
                                              'Male: - 50 to 54 years',
                                              'Male: - 55 to 59 years',
                                              'Male: - 60 and 61 years',
                                              'Male: - 62 to 64 years']),                             
                             ('MALE65P',     ['Male: - 65 and 66 years', 
                                              'Male: - 67 to 69 years', 
                                              'Male: - 70 to 74 years', 
                                              'Male: - 75 to 79 years', 
                                              'Male: - 80 to 84 years', 
                                              'Male: - 85 years and over']), 

                             ('FEMALE0_19',  ['Female: - Under 5 years',
                                              'Female: - 5 to 9 years',
                                              'Female: - 10 to 14 years',
                                              'Female: - 15 to 17 years',
                                              'Female: - 18 and 19 years']),                                              
                             ('FEMALE20_29', ['Female: - 20 years',
                                              'Female: - 21 years',
                                              'Female: - 22 to 24 years',
                                              'Female: - 25 to 29 years']),                                              
                             ('FEMALE30_64', ['Female: - 30 to 34 years',
                                              'Female: - 35 to 39 years',
                                              'Female: - 40 to 44 years',
                                              'Female: - 45 to 49 years',
                                              'Female: - 50 to 54 years',
                                              'Female: - 55 to 59 years',
                                              'Female: - 60 and 61 years',
                                              'Female: - 62 to 64 years']),                             
                             ('FEMALE65P',   ['Female: - 65 and 66 years', 
                                              'Female: - 67 to 69 years', 
                                              'Female: - 70 to 74 years', 
                                              'Female: - 75 to 79 years', 
                                              'Female: - 80 to 84 years', 
                                              'Female: - 85 years and over'])
                            ]
                }

    
    def __init__(self):
        '''
        Constructor. 

        '''   
    
    
    def processCensusPopulationEstimates(self, pre2010File, post2010File, fipsList, outfile): 
        """ 
        Reads the Census annual population estimates, which are published
        at a county level, interpolates them to monthly values, and writes
        them into a consolidated file.  
        
        pre2010File - file containing intercensal (retrospective) population 
                      estimates between 2000 and 2010
        post2010File - file containing postcensal population estimates
        fipsList     - the  FIPS codes to process, as (code, countyName)
        outfile - the HDF output file to write to
        
        """
                
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyPop' in keys: 
            outstore.remove('countyPop')
        if '/totalPop' in keys: 
            outstore.remove('totalPop')
            
            
        # count for unique index
        nrows = 0
        
        # loop through counties    
        for fips, countyName, abbreviation in fipsList: 

            # create the output file for annual data
            annual = pd.DataFrame({'YEAR': range(self.POP_EST_YEARS[0], self.POP_EST_YEARS[1]+1)})
            annual['POP'] = 0
            annual.index = range(self.POP_EST_YEARS[0], self.POP_EST_YEARS[1]+1)
                    
            # get raw data, pre-2010, and copy to annual file
            pre2010_raw = pd.read_csv(pre2010File)
            fips_state = fips[:2]
            fips_county = fips[2:]
            pre2010_raw = pre2010_raw[(pre2010_raw['STATE']==int(fips_state)) 
                                    & (pre2010_raw['COUNTY']==int(fips_county))]
            pre2010_raw.index = range(0, len(pre2010_raw))
            
            for year in range(2000, 2010): 
                annual.at[year,'POP'] = pre2010_raw.at[0, 'POPESTIMATE' + str(year)]
                
            
            # get raw data, post-2010
            post2010_raw = pd.read_csv(post2010File, skiprows=1)
            post2010_raw = post2010_raw[post2010_raw['Id2']==int(fips)]
            post2010_raw.index = range(0, len(post2010_raw))
            
            for year in range(2010, self.POP_EST_YEARS[1]+1): 
                annual.at[year,'POP'] = post2010_raw.at[0, 'Population Estimate (as of July 1) - ' + str(year)]
    
            # convert data to monthly
            monthly = self.convertAnnualToMonthly(annual)

            # set the fips code and unique index
            monthly['FIPS'] = fips
            monthly.index = pd.Series(range(nrows,nrows+len(monthly))) 
            nrows += len(monthly)
                                        
            # append to the output store
            outstore.append('countyPop', monthly, data_columns=True)
        
        # calculate the totals
        df = outstore.select('countyPop')
        totals = df.groupby(['MONTH']).aggregate('sum')
        totals = totals.reset_index()
        outstore.append('totalPop', totals, data_columns=True)
        
        # close
        outstore.close()



    def processCensusSampleData(self, acsDir, census2000Dir, fipsList, cpiFile, outfile): 
        """ 
        Reads raw Census Sample (2000 long form and 2005+ ACS)
        data and converts it to a clean list format. 
        
        acsDir   - directory containing raw ACS data files
        census2000Dir - directory containing Census 2000 data
        fipsList     - the  FIPS codes to process, as (code, countyName)
        cpiFile  - file containing consumer price index data
        outfile  - the HDF output file to write to
        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyACS' in keys: 
            outstore.remove('countyACS')
        if '/countyACSannual' in keys: 
            outstore.remove('countyACSannual')
        if '/totalACS' in keys: 
            outstore.remove('totalACS')
        if '/totalACSannual' in keys: 
            outstore.remove('totalACSannual')
        
        # for unique index
        nyears = 0
        nmonths = 0
        
        
        for fips, countyName, abbreviation in fipsList: 
                
            # get the data
            census2000 = self.getCensus2000Table(census2000Dir, fips)
            acsAnnual = self.getACSAnnualTable(acsDir, fips)
            annual = census2000.append(acsAnnual)
                                
            # convert data to monthly
            monthly = self.convertAnnualToMonthly(annual, censusYears=[2000])
            
            # adjust household incomes for inflation
            dfcpi = self.getCPIFactors(cpiFile)
            monthly = pd.merge(monthly, dfcpi, how='left', on=['MONTH'], sort=True)  
            monthly['MEDIAN_HHINC_2010USD'] = monthly['MEDIAN_HHINC'] * monthly['CPI_FACTOR']
            
            # for calculating weighted average across counties
            monthly['HH_TIMES_INC'] = monthly['HH'] * monthly['MEDIAN_HHINC_2010USD']
            
            # calculate mode shares for journey to work data - totals
            modes    = ['DA', 'SR', 'TRANSIT', 'WALK', 'OTHER', 'HOME']
            monthly['total'] = 0.0
            for mode in modes: 
                monthly['total'] = monthly['total'] + monthly['JTW_' + mode]
            for mode in modes: 
                monthly['JTW_' + mode + '_SHARE'] = monthly['JTW_' + mode] / monthly['total']
            monthly.drop('total', axis=1)
                    
            # calculate mode shares for journey to work data - by segment
            prefixes = ['JTW_0VEH_', 'JTW_1PVEH_', 'JTW_EARN0_50_', 'JTW_EARN50P_', 'JTW_EARN0_75_', 'JTW_EARN75P_']
            modes    = ['DA', 'SR', 'TRANSIT', 'WALK_OTHER', 'HOME']
            for prefix in prefixes:
                monthly['total'] = 0.0
                for mode in modes: 
                    monthly['total'] = monthly['total'] + monthly[prefix + mode]
                for mode in modes: 
                    monthly[prefix + mode + '_SHARE'] = monthly[prefix + mode] / monthly['total']
                monthly.drop('total', axis=1)
    
            # get the july data as the annual measures for each year
            monthly['YEAR'] = monthly['MONTH'].apply(lambda x: x.year)
            monthly['M'] = monthly['MONTH'].apply(lambda x: x.month)
            annual = monthly[monthly['M']==7]
            
            # set the fips code
            annual['FIPS'] = fips
            monthly['FIPS'] = fips
            
            # set unique index
            annual.index = pd.Series(range(nyears,nyears+len(annual))) 
            nyears += len(annual)

            monthly.index = pd.Series(range(nmonths,nmonths+len(monthly))) 
            nmonths += len(monthly)
            
            # append to the output store
            outstore.append('countyACS', monthly, data_columns=True)
            outstore.append('countyACSannual', annual, data_columns=True)
            
            
        # calculate the totals
        df = outstore.select('countyACS')
        totals = df.groupby(['MONTH']).aggregate('sum')
        totals = totals.reset_index()
        totals['MEDIAN_HHINC_2010USD'] = totals['HH_TIMES_INC'] / totals['HH']
        outstore.append('totalACS', totals, data_columns=True)    
            
        df = outstore.select('countyACSannual')   
        totals = df.groupby(['MONTH']).aggregate('sum')
        totals = totals.reset_index()
        totals['MEDIAN_HHINC_2010USD'] = totals['HH_TIMES_INC'] / totals['HH']
        outstore.append('totalACSannual', totals, data_columns=True)
        

        # close
        outstore.close()
        
        
    def getCensus2000Table(self, census2000Dir, fullFips): 
        """ 
        Fills a table of annual Census 2000 data (1 row). 
        
        census2000Dir - directory containing raw data files
        fullFips      - the  FIPS code of interest
        
        """      
        
        # create the output file for annual data, with blanks for 2001-2004
        years = range(2000, 2005)
        year = 2000
        annual = pd.DataFrame({'YEAR': years})
        annual.index = years

        # loop through the tables and get the data
        for table, fields in self.CENSUS2000_EQUIV.iteritems():
            
            # initialize the output container
            for outfield, infields in fields: 
                annual[outfield] = np.NaN
                                    
            # different patterns for CTTP vs. main files
            if table.startswith('1-'): 
                pattern = census2000Dir + '/' + table + '/*' + table + '.csv'
                skiprows = 0
                fips = int(str(fullFips)[-2:])
                countyId = 'COUNTY'
                    
            else:                
                pattern = census2000Dir + '/' + table + '/DEC_' + str(year)[2:] + '*_with_ann.csv'
                skiprows = 1
                fips = int(fullFips)
                countyId = 'Id2'

            infiles = glob.glob(pattern)
                
            if len(infiles)!=1: 
                raise IOError('Wrong number of files matching pattern: ' + pattern)
            else: 
                print (infiles[0])
                df = pd.read_csv(infiles[0], skiprows=skiprows)

                # get the data relevant to this county
                # and set the index equal to the fips code
                df = df[df[countyId]==fips]
                df.index = df[countyId]
                                        
                # copy the data over
                for outfield, infields in fields: 
                    if isinstance(infields, list):                              
                        annual.at[year, outfield] = df.at[fips, infields[0]]   
                        for infield in infields[1:]:                                                  
                            annual.at[year, outfield] += float(df.at[fips, infield])                                
                    else: 
                        annual.at[year, outfield] = df.at[fips, infields]
                        
        return annual
        
    
    def getACSAnnualTable(self, acsDir, fips): 
        """ 
        Fills a table of annual ACS data. 
        
        acsDir - directory containing raw data files
        fips   - the  FIPS code of interest
        
        """
        
        fips = int(fips)

        # create the output file for annual data
        years = range(self.ACS_YEARS[0], self.ACS_YEARS[1]+1)
        annual = pd.DataFrame({'YEAR': years})
        annual.index = years

        # loop through the tables and get the data
        for table, fields in self.ACS_EQUIV.iteritems():
            
            # initialize the output container
            for outfield, infields in fields: 
                annual[outfield] = np.NaN
                
            # open the table specific to each year
            for year in years: 
                pattern = acsDir + '/' + table + '/ACS_' + str(year)[2:] + '*_with_ann.csv'
                infiles = glob.glob(pattern)
                
                if len(infiles)!=1: 
                    raise IOError('Wrong number of files matching pattern: ' + pattern)
                else: 
                    print (infiles[0])
                    df = pd.read_csv(infiles[0], skiprows=1)

                    # get the data relevant to this county
                    # and set the index equal to the fips code
                    df = df[df['Id2']==fips]
                    if 'Population Group' in df.columns: 
                        df = df[df['Population Group']=='Total population']
                    df.index = df['Id2']
                    
                    # normalize the column names
                    colNames = {}
                    for oldName in df.columns: 
                        newName = oldName.replace(str(year), 'YYYY')   
                        newName = newName.replace('Number; ', '')
                        newName = newName.replace('Population 16 years and over - ', '')
                        newName = newName.replace('(IN YYYY INFLATION-ADJUSTED DOLLARS) - Total households -', '(IN YYYY INFLATION-ADJUSTED DOLLARS) -')
                        newName = newName.replace('Total: - ', '')
                        newName = newName.replace('or more vehicles available', 'vehicles available')
                        newName = newName.replace('Total population - AGE', 'AGE')                      
                        newName = newName.replace('Total population - SELECTED AGE CATEGORIES', 'SELECTED AGE CATEGORIES') 
                        newName = newName.replace('Total population - SUMMARY INDICATORS', 'SUMMARY INDICATORS') 
                        
                        colNames[oldName] = newName
                    df = df.rename(columns=colNames) 
                    
                    # copy the data over
                    for outfield, infields in fields: 
                        if isinstance(infields, list):  
                            
                            annual.at[year, outfield] = df.at[fips, infields[0]]   
                            for infield in infields[1:]:                               
                                # special case for one problematic table
                                if table=='B08141': 
                                    if not infield in df.columns: 
                                        continue                                                                
                                annual.at[year, outfield] += float(df.at[fips, infield])                                
                        else: 
                            annual.at[year, outfield] = df.at[fips, infields]
                    
                    # deal with sepcial case for age, to convert percents to total counts
                    if table=='S0101': 
                        annual['MALE0_19']  = annual['MALE'] * annual['MALE_PCT0_19'] / 100.  
                        annual['MALE20_29'] = annual['MALE'] * annual['MALE_PCT20_29'] / 100.  
                        annual['MALE30_64'] = annual['MALE'] * annual['MALE_PCT30_64'] / 100.  
                        annual['MALE65P']   = annual['MALE'] * annual['MALE_PCT65P'] / 100. 
                        
                        annual['FEMALE0_19']  = annual['FEMALE'] * annual['FEMALE_PCT0_19'] / 100.  
                        annual['FEMALE20_29'] = annual['FEMALE'] * annual['FEMALE_PCT20_29'] / 100.  
                        annual['FEMALE30_64'] = annual['FEMALE'] * annual['FEMALE_PCT30_64'] / 100.  
                        annual['FEMALE65P']   = annual['FEMALE'] * annual['FEMALE_PCT65P'] / 100.      
                                                
        return annual
        

    def processHousingUnitsData(self, completionsFiles, census2010File, outfile): 
        """ 
        Reads raw housing completions data and converts it to a clean list format. 
        
        infile   - input csv file
        outfile  - the HDF output file to write to
        
        """
        
        # San Francisco only
        fips = '06075'
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyHousingUnits' in keys: 
            outstore.remove('countyHousingUnits')
        
        # count for unique index
        nrows = 0
                    
        # create the output container
        numYears = self.HU_YEARS[1] - self.HU_YEARS[0] + 1
        months = pd.date_range(str(self.HU_YEARS[0]-1) + '-12-31', 
                periods=12*numYears, freq='M') + pd.DateOffset(days=1)
        dfout = pd.DataFrame({'YEAR': months.year, 'MONTH': months, 'NETUNITS':0})
        
        # read and append each file
        for infile in completionsFiles: 
    
            # read the data
            df = pd.read_csv(infile)
                
            # if the year is not in the columns, fill it in as appropriate
            if not 'YEAR' in df.columns: 
                basename = os.path.basename(infile)
                year = int(basename[:4])
                df['YEAR'] = year
                
            # convert the dates
            df['ACTUAL_DATE'] = df['ACTDATE'].apply(convertToDate)
            df['MONTH'] = df['ACTUAL_DATE'].apply(convertDateToMonth)
                
            # split the records between those with an exact date, and 
            # those that only have a year
            dfExact = df[df['MONTH'].apply(pd.notnull)]
            dfNotExact = df[df['MONTH'].apply(pd.isnull)]        
                
            #group and resample to monthly
            monthlyAgg = dfExact.groupby('MONTH').aggregate(sum)
            monthlyAgg = monthlyAgg.reset_index()
            annualAgg = dfNotExact.groupby('YEAR').aggregate(sum)
            annualAgg = annualAgg.reset_index()
                
            # merge the data.  If missing on RHS, then they are zeros. 
            dfout = pd.merge(dfout, monthlyAgg, how='left', on=['MONTH'], sort=True, suffixes=('', '_MONTHLY')) 
            dfout = pd.merge(dfout, annualAgg, how='left', on=['YEAR'], sort=True, suffixes=('', '_ANNUAL')) 
            dfout = dfout.fillna(0)
                
            # accumulate the totals, distributing annual data throughout the year
            dfout['NETUNITS'] += dfout['NETUNITS_MONTHLY']
            dfout['NETUNITS'] += dfout['NETUNITS_ANNUAL'] / 12
                
            dfout = dfout[['YEAR', 'MONTH', 'NETUNITS']]
            
            
        # get the housing units from the Census 2010 data
        units2010 = self.getCensus2010HousingUnits(census2010File, fips)
        dfout = pd.merge(dfout, units2010, how='left', on=['MONTH'], sort=True, suffixes=('', '_2010')) 
            
        # fill in the totals
        units = dfout['UNITS'].tolist()
        net = dfout['NETUNITS'].tolist()
            
        for i in range(1, len(units)):
            if (pd.notnull(units[i-1])):
                units[i] = round(units[i-1] + net[i-1],0)
        for i in range(len(units)-1, -1, -1):
            if (pd.isnull(units[i])):
                units[i] = round(units[i+1] - net[i],0)
            
        dfout['UNITS'] = units

        # these data are for San Francisco only
        dfout['FIPS'] = fips
                                            
        # write the output
        outstore.append('countyHousingUnits', dfout, data_columns=True)   

        outstore.close()
        

    def getCensus2010HousingUnits(self, census2010File, fips): 
        """
        Gets the number of housing units, according to the Census 2010
        100% enumeration.  

        census2010File - the file for DP01 summary file at county level. 
        fips           - the  FIPS code of interest
        """        
        
        fips = int(fips)

        df = pd.read_csv(census2010File, skiprows=1)
        
        df = df[df['Id2']==fips]
        df.index = df['Id2']
        
        units = df.at[fips, 'Number; HOUSING OCCUPANCY - Total housing units']
        month = pd.Timestamp('2010-04-01')

        dfout = pd.DataFrame({'MONTH': [month], 'UNITS':[units]})

        return dfout
        

    def processQCEWData(self, inputDir, fipsList, cpiFile, outfile): 
        """ 
        Reads raw QCEW data and converts it to a clean list format. 
        
        inputDir - directory containing raw data files
        fipsList     - the  FIPS codes to process, as (code, countyName)
        outfile  - the HDF output file to write to
        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/countyEmp' in keys: 
            outstore.remove('countyEmp')
        if '/totalEmp' in keys: 
            outstore.remove('totalEmp')
       
        # count for unique index
        nrows = 0
            
        for fips, countyName, abbreviation in fipsList: 
                 
            # create an empty dataframe with the right fields
            dfout = pd.DataFrame()
            
            # get the appropriate data
            pattern = inputDir + '*.q1-q*.by_area/*.q1-q* ' + fips + '*.csv'
            infiles = glob.glob(pattern)
                
            for infile in infiles: 
                print ('Reading QCEW data in ' + infile)
                    
                df_allrows = pd.read_csv(infile)
                
                # first get the average earnings for all industries
                # own_code 0 is all ownership categories
                dfin = df_allrows[(df_allrows['own_code']==0) & (df_allrows['industry_title']=='Total, all industries')]
                
                year = dfin['year'][0]
                months = pd.date_range(str(year-1) + '-12-31', periods=12, freq='M') + pd.DateOffset(days=1)
    
                df = pd.DataFrame({'MONTH': months})
                df['AVG_MONTHLY_EARNINGS'] = np.NaN
                
                # check which quarters are included in the file
                quarters = dfin['qtr'].unique()
                
                # copy the earnings data into straight file and convert weekly to monthly
                if 1 in quarters: 
                    df.at[0,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==1]['avg_wkly_wage']   # jan
                if 2 in quarters:     
                    df.at[3,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==2]['avg_wkly_wage']   # mar
                if 3 in quarters: 
                    df.at[6,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==3]['avg_wkly_wage']   # jun
                if 4 in quarters: 
                    df.at[9,'AVG_MONTHLY_EARNINGS'] = dfin[dfin['qtr']==4]['avg_wkly_wage']   # oct        

                #TODO  check on this.  
                df['AVG_MONTHLY_EARNINGS'] = df['AVG_MONTHLY_EARNINGS'] * (12.0 / 3.0)
                
                # for each industry, fill in the columns as appropriate
                industry_equiv = [
                    ('TOTEMP',         '10'),                # Total, all industries
                    ('RETAIL_EMP',  '44-45'),                # Retail trade
                    ('EDHEALTH_EMP', '1025'),                # Education and health services
                    ('LEISURE_EMP',  '1026')                 # Leisure and hospitality    
                    ]                                        
    
                for col, industry_code in industry_equiv:                
                    df[col] = np.NaN
                    
                    # I need to add it up for the specific ownership titles
                    # own_code indicates type of government or private sector.  >0 is all (excluding sum of them all)
                    dfin = df_allrows[(df_allrows['own_code']>0) & (df_allrows['industry_code']==industry_code)]
                    
                    # group across ownership categories
                    grouped = dfin.groupby('qtr')
                    agg = grouped.agg('sum')
                    
                    # fill in the actual column values
                    if 1 in quarters: 
                        df.at[0,col] = agg.at[1,'month1_emplvl']   # jan
                        df.at[1,col] = agg.at[1,'month2_emplvl']   # feb
                        df.at[2,col] = agg.at[1,'month3_emplvl']   # mar
                    if 2 in quarters: 
                        df.at[3,col] = agg.at[2,'month1_emplvl']   # apr
                        df.at[4,col] = agg.at[2,'month2_emplvl']   # may
                        df.at[5,col] = agg.at[2,'month3_emplvl']   # jun
                    if 3 in quarters: 
                        df.at[6,col] = agg.at[3,'month1_emplvl']   # jul
                        df.at[7,col] = agg.at[3,'month2_emplvl']   # aug
                        df.at[8,col] = agg.at[3,'month3_emplvl']   # sep
                    if 4 in quarters: 
                        df.at[9,col] = agg.at[4,'month1_emplvl']   # oct
                        df.at[10,col]= agg.at[4,'month2_emplvl']   # nov
                        df.at[11,col]= agg.at[4,'month3_emplvl']   # dec
                
                # calculate OTHER_EMP based on the difference from the total
                df['OTHER_EMP'] = df['TOTEMP'] - df['RETAIL_EMP'] - df['EDHEALTH_EMP'] - df['LEISURE_EMP']
                
                # append to the full dataframe
                dfout = dfout.append(df, ignore_index=True)
                
            # interpolate from quarterly to monthly values
            dfout['AVG_MONTHLY_EARNINGS'] = dfout['AVG_MONTHLY_EARNINGS'].interpolate()
                
            # adjust for inflation
            dfcpi  = self.getCPIFactors(cpiFile)
            dfjoin = pd.merge(dfout, dfcpi, how='left', on=['MONTH'], sort=True)  
            dfout['AVG_MONTHLY_EARNINGS_2010USD'] = dfjoin['AVG_MONTHLY_EARNINGS'] * dfjoin['CPI_FACTOR']

            # for calculating a weighted average of earnings
            dfout['EMP_TIMES_EARNINGS'] = dfout['TOTEMP'] * dfout['AVG_MONTHLY_EARNINGS_2010USD']
            
            # only keep non-missing data
            dfout = dfout[dfout['TOTEMP']>0]
            
            # set the fips code and a unqiue index
            dfout['FIPS'] = fips
            dfout.index = pd.Series(range(nrows,nrows+len(dfout))) 
            nrows += len(dfout)
                    
            # write the output
            outstore.append('countyEmp', dfout, data_columns=True)        

        # calculate the totals
        df = outstore.select('countyEmp')
        totals = df.groupby(['MONTH']).aggregate('sum')
        totals = totals.reset_index()
        totals['AVG_MONTHLY_EARNINGS_2010USD'] = totals['EMP_TIMES_EARNINGS'] / totals['TOTEMP'] 
        outstore.append('totalEmp', totals, data_columns=True)

        # close
        outstore.close()
        


        
    def processLODES(self, inputDir, lodesType, xwalkFile, fipsList, outfile): 
        '''
        Processes data from the LODES (LEHD Origin-Destination Employment Statistics)
        files.  Processed for SF county as a whole.
        
        inputDir - directory containing input CSV files
        lodesType - RAC, WAC or OD
                    OD file processed specifically for intra-county vs inter-county flows
        xwalkFile - file containing the geography crosswalk from LODES
        fipsList     - the  FIPS codes to process, as (code, countyName)
        outfile - HDF file to write to
        '''
        # set characteristics for later
        key = 'lodes' + lodesType
        totalKey = 'lodes' + lodesType + 'total'
            
        if lodesType=='RAC': 
            geoCol = 'h_geocode'
            wrkemp = 'WORKERS'
            filePattern = inputDir + '/RAC/ca_rac_S000_JT00_'
                
        elif lodesType=='WAC':
            geoCol = 'w_geocode'
            wrkemp = 'EMP'
            filePattern = inputDir + '/WAC/ca_wac_S000_JT00_'
                
        elif lodesType=='OD':
            hgeoCol = 'h_geocode'
            wgeoCol = 'w_geocode'
            wrkempList = ['INTRA', 'IN', 'OUT']
            filePattern = inputDir + '/OD/ca_od_main_JT00_'
                
            
        # remove the existing keys
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/' + key in keys: 
            outstore.remove(key)
        if '/' + totalKey in keys: 
            outstore.remove(totalKey)
        if lodesType=='WAC': 
            if '/lodesFactors' in keys: 
                outstore.remove('lodesFactors')
                
        # read the geography crosswalk
        xwalk = pd.read_csv(xwalkFile)
        xwalk['cty'] = xwalk['cty'].astype(int)
        xwalk = xwalk[['tabblk2010', 'cty']]
            
        # unique count for index
        nrows = 0
        
        # loop through counties
        for fips, countyName, abbreviation in fipsList: 
            fipsInt = int(fips)
            
            # create the output file for annual data
            years = range(self.LODES_YEARS[0], self.LODES_YEARS[1]+1)
            annual = pd.DataFrame({'YEAR': years})
            annual.index = years
            
            if lodesType=='RAC' or lodesType=='WAC': 
                annual[wrkemp] = np.NaN          # total workers
                
                annual[wrkemp+'_EARN0_15'] = np.NaN  # Number of workers with earnings $1250/month or less
                annual[wrkemp+'_EARN15_40']= np.NaN  # Number of workers with earnings $1251/month to $3333/month
                annual[wrkemp+'_EARN40P']  = np.NaN  # Number of workers with earnings greater than $3333/month
                
                annual[wrkemp+'_RETAIL']   = np.NaN  # Number of workers in retail sector
                annual[wrkemp+'_EDHEALTH'] = np.NaN  # Number of workers in education and health sector
                annual[wrkemp+'_LEISURE']  = np.NaN  # Number of workers in leisure and hospitality sector
                annual[wrkemp+'_OTHER']    = np.NaN  # Number of workers in other sectors
            
            elif lodesType=='OD': 
                for wrkemp in wrkempList: 
                    annual[wrkemp] = np.NaN          # total workers
                    
                    annual[wrkemp+'_EARN0_15'] = np.NaN  # Number of workers with earnings $1250/month or less
                    annual[wrkemp+'_EARN15_40']= np.NaN  # Number of workers with earnings $1251/month to $3333/month
                    annual[wrkemp+'_EARN40P']  = np.NaN  # Number of workers with earnings greater than $3333/month
                    
            
            # get the data for each year
            for year in years: 
                
                # read the data and aggregate to county level
                infile = filePattern + str(year) + '.csv' 
                if os.path.isfile(infile):
                        
                    print ('Reading LODES data in ' + infile)            
                    df = pd.read_csv(infile)            
                    
                    # one dimensional processing for RAC and WAC
                    if lodesType=='RAC' or lodesType=='WAC': 
                        df = pd.merge(df, xwalk, how='left', left_on=geoCol, right_on='tabblk2010')            
                        df = df[df['cty']==fipsInt]            
                        agg = df.groupby('cty').agg('sum')
                        
                        # copy over the appropriate fields
                        annual.at[year, wrkemp] = agg.at[fipsInt, 'C000']        
                        
                        annual.at[year, wrkemp+'_EARN0_15'] = agg.at[fipsInt, 'CE01']
                        annual.at[year, wrkemp+'_EARN15_40']= agg.at[fipsInt, 'CE02'] 
                        annual.at[year, wrkemp+'_EARN40P']  = agg.at[fipsInt, 'CE03'] 
                        
                        annual.at[year, wrkemp+'_RETAIL']   = agg.at[fipsInt, 'CNS07'] 
                        annual.at[year, wrkemp+'_EDHEALTH'] = agg.at[fipsInt, 'CNS15'] + agg.at[fipsInt, 'CNS16'] 
                        annual.at[year, wrkemp+'_LEISURE']  = agg.at[fipsInt, 'CNS17'] + agg.at[fipsInt, 'CNS18'] 
                        annual.at[year, wrkemp+'_OTHER']    = (annual.at[year, wrkemp] 
                                                            -annual.at[year, wrkemp+'_RETAIL']
                                                            -annual.at[year, wrkemp+'_EDHEALTH']
                                                            -annual.at[year, wrkemp+'_LEISURE']
                                                            )
                    
                    # for OD, keep different values for each option
                    elif lodesType=='OD': 
                        df = pd.merge(df, xwalk, how='left', left_on=hgeoCol, right_on='tabblk2010')   
                        df = pd.merge(df, xwalk, how='left', left_on=wgeoCol, right_on='tabblk2010', suffixes=('_h', '_w'))           
            
                        for wrkemp in wrkempList:               
                            
                            # intra-county
                            if wrkemp == 'INTRA': 
                                selected = df[(df['cty_h']==fipsInt) & (df['cty_w']==fipsInt)]                             
                                agg = selected.groupby('cty_h').agg('sum')
                            elif wrkemp == 'IN': 
                                selected = df[(df['cty_h']!=fipsInt) & (df['cty_w']==fipsInt)]                             
                                agg = selected.groupby('cty_w').agg('sum')
                            elif wrkemp == 'OUT': 
                                selected = df[(df['cty_h']==fipsInt) & (df['cty_w']!=fipsInt)]                             
                                agg = selected.groupby('cty_h').agg('sum')
                            
                            # copy over the appropriate fields
                            annual.at[year, wrkemp] = agg.at[fipsInt, 'S000']        
                            
                            annual.at[year, wrkemp+'_EARN0_15'] = agg.at[fipsInt, 'SE01']
                            annual.at[year, wrkemp+'_EARN15_40']= agg.at[fipsInt, 'SE02'] 
                            annual.at[year, wrkemp+'_EARN40P']  = agg.at[fipsInt, 'SE03'] 
                        
            # convert data to monthly
            monthly = self.convertAnnualToMonthly(annual)
            monthly['FIPS'] = fips
                        
            # scale to be consistent with QCEW data
            # factor is based on the ratio of QCEW to WAC
            if lodesType=='WAC': 
                self.setLODEStoQCEWFactors(monthly, outstore)
    
            if lodesType=='RAC' or lodesType=='WAC': 
                scaled = self.scaleLODEStoQCEW(monthly, lodesType, outstore, wrkemp)        
            elif lodesType=='OD': 
                scaled = monthly
                for wrkemp in wrkempList: 
                    scaled = self.scaleLODEStoQCEW(scaled, lodesType, outstore, wrkemp)  
        
            # set the fips code and a unqiue index
            scaled['FIPS'] = fips
            scaled.index = pd.Series(range(nrows,nrows+len(scaled))) 
            nrows += len(scaled)
                                                            
            # append to the output store
            outstore.append(key, scaled, data_columns=True)
        
        # totals
        df = outstore.select(key)
        totals = df.groupby(['MONTH']).aggregate('sum')
        totals = totals.reset_index()
        outstore.append(totalKey, totals, data_columns=True)

        # close
        outstore.close()
        

    def setLODEStoQCEWFactors(self, wac, outstore):
        '''
        Determines factors to scale LODES data to be consistent with QCEW data 
        based on the ratio of QCEW to WAC. 
        '''

        qcew = outstore.select('countyEmp')
        
        # calculate the factors
        factors = pd.merge(wac, qcew, how='left', on=['MONTH', 'FIPS'], sort=True, suffixes=('_WAC', '_QCEW'))  
        factors['TOT_FACTOR']     = 1.0 * factors['TOTEMP'] / factors['EMP']
        factors['RETAIL_FACTOR']  = 1.0 * factors['RETAIL_EMP'] / factors['EMP_RETAIL'] 
        factors['EDHEALTH_FACTOR']= 1.0 * factors['EDHEALTH_EMP'] / factors['EMP_EDHEALTH'] 
        factors['LEISURE_FACTOR'] = 1.0 * factors['LEISURE_EMP'] / factors['EMP_LEISURE'] 
        factors['OTHER_FACTOR']   = 1.0 * factors['OTHER_EMP'] / factors['EMP_OTHER'] 
        
        factors = factors[['MONTH', 'FIPS', 'TOT_FACTOR', 'RETAIL_FACTOR', 'EDHEALTH_FACTOR', 'LEISURE_FACTOR', 'OTHER_FACTOR']]
        
        # write the data
        keys = outstore.keys()
        outstore.append('lodesFactors', factors, data_columns=True)
        

    def scaleLODEStoQCEW(self, monthly, lodesType, store, wrkemp):
        '''
        Scales LODES data to be consistent with QCEW data.
        Based on factors calculated above
        '''
        
        columns = monthly.columns.tolist()

        # get the factors, written above
        factors = store.select('lodesFactors')
        
        # apply the factors
        adj = pd.merge(monthly, factors, how='left', on=['MONTH', 'FIPS'], sort=True, suffixes=('', '_FACTOR'))  
                
        adj[wrkemp] = adj[wrkemp] * adj['TOT_FACTOR']          # total workers
                
        adj[wrkemp+'_EARN0_15'] = adj[wrkemp+'_EARN0_15']  * adj['TOT_FACTOR']
        adj[wrkemp+'_EARN15_40']= adj[wrkemp+'_EARN15_40'] * adj['TOT_FACTOR']  
        adj[wrkemp+'_EARN40P']  = adj[wrkemp+'_EARN40P']   * adj['TOT_FACTOR']
        
        if lodesType=='RAC' or lodesType=='WAC': 
            adj[wrkemp+'_RETAIL']   = adj[wrkemp+'_RETAIL']   * adj['RETAIL_FACTOR']
            adj[wrkemp+'_EDHEALTH'] = adj[wrkemp+'_EDHEALTH'] * adj['EDHEALTH_FACTOR']
            adj[wrkemp+'_LEISURE']  = adj[wrkemp+'_LEISURE']  * adj['LEISURE_FACTOR']
            adj[wrkemp+'_OTHER']    = adj[wrkemp+'_OTHER']    * adj['OTHER_FACTOR']
        
        # keep only the original columns       
        return adj[columns]



    def processAutoOpCosts(self, fuelFile, fleetEfficiencyFile, mileageRateFile, cpiFile, outfile): 
        """ 
        Reads raw QCEW data and converts it to a clean list format. 
        
        fuelFile - file containing data from EIA
        fleetEfficiencyFile - file containing average fleet mpg
        cpiFile  - inflation factors
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/autoOpCost' in keys: 
            outstore.remove('autoOpCost')
        
        # get and merge the data
        fuelPrice = self.getFuelPriceData(fuelFile, cpiFile)
        fleetEfficiency = self.getFleetEfficiencyData(fleetEfficiencyFile)
        irsMileageRate = self.getIRSMileageRates(mileageRateFile, cpiFile)

        dfout = pd.merge(fuelPrice, fleetEfficiency, how='left', on=['MONTH'], sort=True)  
        dfout = pd.merge(dfout, irsMileageRate, how='left', on=['MONTH'], sort=True)  
        
        # expand fleet efficiency to the end of the series
        dfout['FLEET_EFFICIENCY'] = dfout['FLEET_EFFICIENCY'].interpolate()
        
        # calculate the average cost per mile
        dfout['FUEL_COST'] = dfout['FUEL_PRICE'] / dfout['FLEET_EFFICIENCY']
        dfout['FUEL_COST_2010USD'] = dfout['FUEL_PRICE_2010USD'] / dfout['FLEET_EFFICIENCY']
        
        # append to the output store
        outstore.append('autoOpCost', dfout, data_columns=True)
        outstore.close()


    def getFuelPriceData(self, fuelFile, cpiFile): 
        """ 
        Gets the fuel price data and returns it as a dataframe
        
        fuelFile - file containing data from EIA
        cpiFile  - inflation factors
        
        """        
        # get raw data
        df = pd.read_excel(fuelFile, sheetname='Data 4', skiprows=2)
        df = df.rename(columns={
                       'Date': 'MONTH', 
                       'San Francisco All Grades All Formulations Retail Gasoline Prices (Dollars per Gallon)': 'FUEL_PRICE'
                       })
        df = df[['MONTH', 'FUEL_PRICE']]
        
        # normalize to the first day of the month
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=-14))
                
        # adjust the fuel price for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        df['FUEL_PRICE_2010USD'] = df['FUEL_PRICE'] * df['CPI_FACTOR']
        
        # keep only the relevant columns
        df = df[['MONTH', 'FUEL_PRICE', 'FUEL_PRICE_2010USD', 'CPI']]        
        
        return df

    
    def getFleetEfficiencyData(self, fleetEfficiencyFile): 
        """ 
        Gets the average fleet efficiency and returns it as a dataframe
        
        """        

        annual = pd.read_csv(fleetEfficiencyFile, skiprows=1)
        annual = annual.rename(columns={'Light duty vehicle, short wheel base' : 'FLEET_EFFICIENCY'}) 
        annual.index = annual['YEAR']
        annual = annual[['YEAR', 'FLEET_EFFICIENCY']]

        monthly = self.convertAnnualToMonthly(annual)
        
        return monthly
    

    def getIRSMileageRates(self, mileageRateFile, cpiFile):
        """ 
        Gets the IRS mileage reimbursement rate representing the marginal
        cost of miles driven.  
        
        mileageRateFile - file containing data from IRS
        cpiFile  - inflation factors
        
        """        
        # get raw data
        df = pd.read_csv(mileageRateFile)

        # copy the data of interest, converting from cents to dollars
        df['MONTH'] = df['PeriodStart'].apply(pd.to_datetime)
        df['IRS_MILEAGE_RATE'] = df['Medical/Moving'] / 100.0
        
        # extrapolate to get the last fiscal year of monthly data        
        index = np.max(df.index.tolist()) + 1
        month = df['MONTH'].max() + pd.DateOffset(months=11)
        lastRow = pd.DataFrame({'MONTH' : [month]}, index=[index])
        df = df.append(lastRow)
        
        # adjust the rate for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        df['IRS_MILEAGE_RATE_2010USD'] = df['IRS_MILEAGE_RATE'] * df['CPI_FACTOR']
                
        # expand to a monthly, using backfill to keep same rate for whole year
        df = df.set_index(pd.DatetimeIndex(df['MONTH']))
        df = df.resample('M', fill_method='ffill')
        df['MONTH'] = df.index
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # keep only the relevant columns
        df = df[['MONTH', 'IRS_MILEAGE_RATE', 'IRS_MILEAGE_RATE_2010USD']]        
        
        return df


    def processTollCosts(self, tollFile, cpiFile, outfile): 
        """ 
        Processes the toll schedules into a monthly list format. 
        
        tollFile - file containing the input toll rates in nominal dollars
        cpiFile  - inflation factors
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/tollCost' in keys: 
            outstore.remove('tollCost')
        
        # get the data and expand it to monthly
        df = pd.read_csv(tollFile)
                
        # expand to a monthly, using backfill to keep same rate until it changes
        df = df.set_index(pd.DatetimeIndex(df['PeriodStart']))
        df = df.resample('M', fill_method='ffill')
        df['MONTH'] = df.index
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # adjust the rate for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        
        for col in df.select_dtypes(include=[np.number]).columns: 
            df[col + '_2010USD'] = df[col] * df['CPI_FACTOR']

        # append to the output store
        outstore.append('tollCost', df, data_columns=True)
        outstore.close()


    def processParkingCosts(self, parkingRateFile, cpiFile, outfile): 
        """ 
        Processes the parking costs into a monthly list format. 
        
        parkingRateFile - file containing the input toll rates in nominal dollars
        cpiFile  - inflation factors
        outfile  - the HDF output file to write to        
        """
        
        # remove the existing key so we don't overwrite
        outstore = pd.HDFStore(outfile)
        keys = outstore.keys()
        if '/parkingCost' in keys: 
            outstore.remove('parkingCost')
        
        # get the data and expand it to monthly
        df = pd.read_csv(parkingRateFile)
                
        # expand to a monthly, using backfill to keep same rate until it changes
        df = df.set_index(pd.DatetimeIndex(df['PeriodStart']))
        df = df.resample('M', fill_method='ffill')
        df['MONTH'] = df.index
        df['MONTH'] = df['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
        
        # adjust the rate for inflation
        dfcpi = self.getCPIFactors(cpiFile)
        df = pd.merge(df, dfcpi, how='left', on=['MONTH'], sort=True)  
        
        for col in df.select_dtypes(include=[np.number]).columns: 
            df[col + '_2010USD'] = df[col] * df['CPI_FACTOR']

        # append to the output store
        outstore.append('parkingCost', df, data_columns=True)
        outstore.close()


    
    def getCPIFactors(self, cpiFile):
        """ 
        Reads CPI numbers and returns a dataframe with the CPI_FACTOR field
        that can be joined by month and used to adjust monetary values by 
        inflation to 2010 US dollars.  
        
        """
        
        # get the CPI and convert to monthly format
        dfcpi = pd.read_excel(cpiFile, sheetname='BLS Data Series', skiprows=10, index_col=0)
        base = dfcpi.at[2010, 'Annual']
        
        dfcpi = dfcpi.drop(['Annual', 'HALF1', 'HALF2'], axis=1)
        dfcpi = dfcpi.stack()
        dfcpi = dfcpi.reset_index()
        dfcpi = dfcpi.rename(columns={
                             'level_1' : 'monthString', 
                             0 : 'CPI'
                             })
                             
        dfcpi['MONTH'] = '01-' + dfcpi['monthString'] + '-' + dfcpi['Year'].astype('string')
        dfcpi['MONTH'] = dfcpi['MONTH'].apply(pd.Timestamp)
        
        dfcpi['CPI_FACTOR'] = base / dfcpi['CPI']
        
        dfcpi = dfcpi[['MONTH', 'CPI', 'CPI_FACTOR']]
                
        return dfcpi


    def convertAnnualToMonthly(self, annual, censusYears=[]): 
        '''
        Convert annual dataframe to monthly dataframe. 
        Use linear interpolation to interpolate values, and extend to end
        of year.  
        
        '''        

        # extrapolate the first year to get the first 6 months of data
        extraStartYear = int(annual['YEAR'].min() - 1)
        annual.loc[extraStartYear] = np.NaN
        annual.at[extraStartYear, 'YEAR'] = extraStartYear
        for col in annual.columns:
            if np.dtype(annual[col]) == np.dtype('O'): 
                annual.at[extraStartYear, col] = annual.at[extraStartYear+1, col]
            else: 
                annual.at[extraStartYear, col] =(annual.at[extraStartYear+1, col] - 
                                        (annual.at[extraStartYear+2, col] 
                                        -annual.at[extraStartYear+1, col]))

        # extrapolate the final year to get the last 6 months of data
        extraEndYear = int(annual['YEAR'].max() + 1)
        annual.loc[extraEndYear] = np.NaN
        annual.at[extraEndYear, 'YEAR'] = extraEndYear
        for col in annual.columns:
            if np.dtype(annual[col]) == np.dtype('O'): 
                annual.at[extraStartYear, col] = annual.at[extraEndYear-1, col]
            else: 
                annual.at[extraEndYear, col] =(annual.at[extraEndYear-1, col] + 
                                        (annual.at[extraEndYear-1, col] 
                                        -annual.at[extraEndYear-2, col]))
        
        # expand to monthly, and interpolate values
        annual = annual.sort('YEAR')
        annual['MONTH'] = annual['YEAR'].apply(lambda x: pd.Timestamp(str(int(x)) + '-07-01'))
        annual = annual.set_index(pd.DatetimeIndex(annual['MONTH']))

        monthly = annual[['MONTH']].resample('M')
        monthly['MONTH'] = monthly.index
        monthly['MONTH'] = monthly['MONTH'].apply(pd.DateOffset(days=1)).apply(pd.DateOffset(months=-1))
                
        # special case for census years
        for y in censusYears:
            annual = annual.set_index(annual['YEAR'].astype(int))
            annual.at[y, 'MONTH'] = pd.Timestamp(str(int(annual.at[y, 'YEAR'])) + '-04-01')
            annual = annual.set_index(pd.DatetimeIndex(annual['MONTH']))

        monthly = pd.merge(monthly, annual, how='left', on=['MONTH'], sort=True)  
        monthly = monthly.set_index(pd.DatetimeIndex(monthly['MONTH']))   
        
        monthly = monthly.interpolate()
        
        # drop the extraStartYear and extraEndYear
        monthly = monthly[monthly['YEAR']>=extraStartYear+0.5]
        monthly = monthly[monthly['YEAR']<extraEndYear-0.5]
        monthly = monthly.drop('YEAR', 1)
                
        # set a unique index
        monthly.index = pd.Series(range(0,len(monthly)))
                   
        return monthly

    