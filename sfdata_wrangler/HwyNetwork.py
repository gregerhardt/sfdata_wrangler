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

import dta
import math
from pyproj import Proj
from mm.path_inference.structures import State
from mm.path_inference.structures import Path 


def convertLongitudeLatitudeToXY(lon_lat):        
    """
    Converts longitude and latitude to an x,y coordinate pair in
    NAD83 Datum (most of our GIS and CUBE files)
    
    Returns (x,y) in feet.
    """
    FEET_TO_METERS = 0.3048006096012192
    
    (longitude,latitude) = lon_lat

    p = Proj(proj  = 'lcc',
            datum = "NAD83",
            lon_0 = "-120.5",
            lat_1 = "38.43333333333",
            lat_2 = "37.066666666667",
            lat_0 = "36.5",
            ellps = "GRS80",
            units = "m",
            x_0   = 2000000,
            y_0   = 500000) #use kwargs
    x_meters,y_meters = p(longitude,latitude,inverse=False,errcheck=True)

    return (x_meters/FEET_TO_METERS,y_meters/FEET_TO_METERS)

def isInSanFranciscoBox(x_y):    
    """
    Checks whether the x_y point given is within a rectangular box
    drawn around the City of San Francisco.
    """
    (x, y) = x_y
    
    if (x > 5979762.10716
    and y > 2074908.26203
    and x < 6027567.22925
    and y < 2130887.56530):
        return True
    else: 
        return False


def distanceInFeet(position1, position2): 
    """
    Accepts two GPS positions
    
    Returns the distance between the two points.  
    """
        
    dist = math.sqrt(((position1.x-position2.x)**2) 
                   + ((position1.y-position2.y)**2))
    return dist
                 
                           
class HwyNetwork():
    """ 
    Methods used to read and process highway network, and provide
    wrapper functionality around the basic data structure. 
    """

    # consider up to this many links when projecting
    PROJECT_NUM_LINKS = 5               
    
    # all links within this threshold will be considered when projecting
    PROJECT_DIST_THRESHOLD = 100.0    # feet


    def __init__(self):
        """
        Constructor.             
        """   
        
        # The network is of the form dta.Network
        # it will be set below in the read statement. 
        self.net = None
        

    def readDTANetwork(self, inputDir, filePrefix):
        
        # The SanFrancisco network will use feet for vehicle lengths and coordinates, and miles for link lengths
        dta.VehicleType.LENGTH_UNITS= "feet"
        dta.Node.COORDINATE_UNITS   = "feet"
        dta.RoadLink.LENGTH_UNITS   = "miles"

        dta.setupLogging("c:/temp/dta.INFO.log", "c:/temp/visualizeDTAResults.DEBUG.log", logToConsole=False)

        scenario = dta.DynameqScenario()
        scenario.read(inputDir, filePrefix) 
        net = dta.DynameqNetwork(scenario)

        net.read(inputDir, filePrefix)
        
        # initialize costs
        dta.Algorithms.ShortestPaths.initiaxblizeEdgeCostsWithFFTT(net)
        dta.Algorithms.ShortestPaths.initialiseMovementCostsWithFFTT(net)        
        
        self.net = net
        
        
    def project(self, gps_pos):
        """ (abstract) : takes a GPS position and returns a list of states.
        """
        
        #The *roadlink* is the closest :py:class:`RoadLink` instance to (*x*, *y*),
        #the *distance* is the distance between (*x*, *y*) and the *roadlink*, and 
        #*t* is in [0,1] and indicates how far along from the start point and end point
        #of the *roadlink* lies the closest point to (*x*, *y*).
        
        return_tuple = self.net.findNRoadLinksNearestCoords(gps_pos.x, gps_pos.y, 
            n=self.PROJECT_NUM_LINKS, quick_dist=self.PROJECT_DIST_THRESHOLD)
        
        states = []
        for rt in return_tuple: 
            (roadlink, distance, t) = rt
            offset = t * roadlink.getLengthInCoordinateUnits()
            state = State(roadlink.getId(), offset, distFromGPS=distance)
            states.append(state)
                    
        return states
        

    # TODO - update to get multiple paths
    def getPaths(self, s1, s2):
        """ Returns a set of candidate paths between state s1 and state s3.
        Arguments:
        - s1 : a State object
        - s2 : a State object
        """        
        
        link1 = self.net.getLinkForId(s1.link_id)
        link2 = self.net.getLinkForId(s2.link_id)    
        
        links = dta.Algorithms.ShortestPaths.getShortestPathBetweenLinks(
                self.net, link1, link2, runSP=True)       
                
        if (links==None):
            #print 'No valid path between links ', s1.link_id, ' and ', s2.link_id
            return [None]
            
        link_ids = []             
        for link in links:
            link_ids.append(link.getId())        
        
        path = Path(s1, link_ids, s2)        
        return [path]

    def getPathsBetweenCollections(self, sc1, sc2):
        """ Returns a set of candidate paths between all pairs of states
        in the two state collections.
        Arguments:
        - s1 : a StateCollection object
        - s2 : a StateCollection object
        """
        trans1 = []
        trans2 = []
        paths = []
        n1 = len(sc1.states)
        n2 = len(sc2.states)
        num_paths = 0
        for i1 in range(n1):
            for i2 in range(n2):
                ps = self.getPaths(sc1.states[i1], sc2.states[i2])
                for path in ps:
                    trans1.append((i1, num_paths))
                    trans2.append((num_paths, i2))
                    paths.append(path)
                    num_paths += 1
        return (trans1, paths, trans2)


    def getPathFreeFlowTTInSeconds(self, path):
        """ Returns the free-flow travel time of the path in seconds.
        
        Arguments: a path_inference.structures.Path object
        """
        
        # frist get the total time across all links
        tot_tt = 0.0
        for link_id in path.links:
            link = self.net.getLinkForId(link_id)
            tot_tt += 60.0 * link.getFreeFlowTTInMin()
        
        # then adjust for the position along the first link
        first_link = self.net.getLinkForId(path.start.link_id)
        first_tt = 60.0 * first_link.getFreeFlowTTInMin()
        firstOffsetRatio = self.getLinkOffsetRatio(path.start)
        tot_tt -= first_tt * firstOffsetRatio
        
        # then adjust for the position along the last link                
        last_link = self.net.getLinkForId(path.end.link_id)
        last_tt = 60.0 * last_link.getFreeFlowTTInMin()
        lastOffsetRatio = self.getLinkOffsetRatio(path.end)
        tot_tt -= last_tt * (1 - lastOffsetRatio)
        
        return tot_tt
    
    def getLinkOffsetRatio(self, state):
        """ Returns the offset ratio         
        offset ratio is in [0,1] and indicates how far along from the 
        start point and end point
        """
        link = self.net.getLinkForId(state.link_id)
        dist = link.getLengthInCoordinateUnits()
        ratio = state.offset / dist
        return ratio
        

    def allocatePathTravelTimeToFullLinks(self, path, start_time, end_time):
        """ Returns an array of link travel times corresponding to the 
        links in the path.  
        
        For the first and last link, the travel time is what would be 
        required to traverse the whole link, not just the portion that is
        actually travelled in the trajectory.  Thus, the sum of the link 
        times will be higher than the end_time - start_time.  It is up to 
        the caller of this fuction to account for first and last link issues.           
        
        Arguments: a path_inference.structures.Path object
                   a datetime object for the start time
                   a datetime object for the end time
        """
        
        # The adjustment is the ratio of the observed travel time
        # to the free flow time
        tot_tt = (end_time - start_time).total_seconds()
        tot_ff_time = self.getPathFreeFlowTTInSeconds(path)
        ratio = tot_tt / tot_ff_time

        # apply that adjustment factor to each link
        link_tt = []
        for link_id in path.links:
            link = self.net.getLinkForId(link_id)
            tt = ratio * 60.0 * link.getFreeFlowTTInMin()
            link_tt.append(tt)
        
        return link_tt
        
        
        