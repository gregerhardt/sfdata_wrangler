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
import numpy as np
import pandas as pd

from SFMuniDataHelper import SFMuniDataHelper
from DataFrameViewer import DataFrameViewer



if __name__ == "__main__":
    
    # read the data
    sfmuniHelper = SFMuniDataHelper()
    df1 = sfmuniHelper.read_stp("C:/CASA/Data/MUNI/SFMTA Data/Raw STP Files/1310.stp")
        
    # write the data
    sfmuniHelper.write_hdf(df1, "C:/CASA/DataExploration/sfmuni.h5")

    # read it back in
    df2 = sfmuniHelper.read_hdf("C:/CASA/DataExploration/sfmuni.h5")    

    # let the user view the first 1000 rows
    vw = DataFrameViewer()
    vw.view(df2[1:1000])