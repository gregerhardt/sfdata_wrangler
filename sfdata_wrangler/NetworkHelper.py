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

import networkx as nx
import datetime
                    
                                    
class NetworkHelper():
    """ 
    Methods used to read and process highway network. 
    """

    def __init__(self):
        """
        Constructor.             
        """   
                    
    def readShapeFile(self, infile):
        """
        Reads a link shapefile and converts it to network structure.
        
        infile  - link shapefile from travel model
        """
        
        print datetime.datetime.now(), 'Reading network in file: ', infile
        
        net = nx.read_shp(infile)
        
    
        