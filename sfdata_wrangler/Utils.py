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

def cleanCrosstab(rows, cols, values, aggfunc=sum): 
    """ 
    Performs a crosstab on the rows, cols and values specified.
    
    In the end, if there are no observations, it the value is zero, but
    if there are observations with a np.nan value, then those remain as
    missing values.  
    
    Also, adds in proper row and column totals
    """
        
    t = pd.crosstab(rows, cols, values.fillna(np.inf), aggfunc=aggfunc)
    t.replace(to_replace=np.nan, value=0, inplace=True)
    t.replace(to_replace=np.inf, value=np.nan, inplace=True)
    t['Total'] = t.sum(axis=1)
    t = t.append(pd.Series(t.sum(axis=0), name='Total'))
    return t
        