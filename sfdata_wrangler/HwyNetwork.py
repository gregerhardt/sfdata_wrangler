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

import sys
import dta
import math
import operator
import numpy as np
import scipy as sp
import pandas as pd
from scipy.sparse import csr_matrix
from pyproj import Proj
from mm.path_inference.structures import State
from mm.path_inference.structures import Path 

try: 
    import rtree 
except(WindowsError):
    print "Be sure libspatialindex is installed on your system. \
             1. Download it from: http://download.osgeo.org/libspatialindex/ \
             2. Put the DLLs in your system PATH \
             3. Make sure the 32/64 bit DLL is consistent with running \
                32/64 bit python.  \
             4. rtree will look for spatialindex1_c.dll, so rename if needed \
                from something like: spatialindex_c-64.dll. \
             5. But keep something like spatialindex-64.dll with its original \
                name because the first DLL will look for this one. " 
           
    raise


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
    # in case you are in a 'corner' between an EW and a NS street, you 
    # want this to be at least 4 so you get both streets in both directions.
    # here I choose one more for good measure. 
    PROJECT_NUM_LINKS = 5               
    
    # links within this threshold will be considered when projecting
    # city blocks in the financial district are about 250 ft
    # use half that distance as a threshold for selecting links. 
    # also, the GPS error seems to be about 60 ft, so it should be at 
    # least that much
    PROJECT_DIST_THRESHOLD = 150.0    # feet

    # turn penalties are used when calculating shortest paths on links
    # (but not on nodes), and discourage zig-zag paths through the grid
    # network
    LEFT_TURN_PENALTY = 30.0    # seconds
    RIGHT_TURN_PENALTY = 10.0
    U_TURN_PENALTY = 60.0

    # only consider paths within this ratio of the time between gps points. 
    # So if there are 60 seconds between GPS points, we will only consider
    # paths with a free flow time of no more than 120 seconds for a 
    # TIME_LIMIT_FACTOR of 2.0.  The time is inclusive of turn penalties
    # and the full travel time on the first and last links, so be a bit 
    # generous. The minimum ensures that we will still consider paths up
    # to that limit. 
    TIME_LIMIT_FACTOR = 2.0
    TIME_LIMIT_MINIMUM = 60.0

    def __init__(self):
        """
        Constructor.             
        """   
        
        # The network is of the form dta.Network
        # it will be set below in the read statement. 
        self.net = None
        
        # This is an rtree index with one entry for each road link
        # It is used for fast nearest neighbour queries
        self.linkSpatialIndex = None
        
        """
        These options are for building shortest paths between nodes.  The 
        paths between nodes do not consider turn restrictions or penalties. 
        
        They are not currently used. 
        """
        # a dictionary lookup between the node IDs and
        # the graph index for skim and pred
        self.n2i = None
        self.i2n = None
        
        # The N x N matrix of costs between graph nodes. skim[i,j] gives 
        # the shortest cost from point i to point j along the graph.
        self.nodeSkim = None
        
        # The N x N matrix of predecessors, which can be used to reconstruct 
        # the shortest paths. Row i of the predecessor matrix contains information 
        # on the shortest paths from point i: each entry predecessors[i, j] 
        # gives the index of the previous node in the path from point i to point j. 
        # If no path exists between point i and j, then predecessors[i, j] = -9999
        self.nodePred = None
        
        """
        These options are for building shortest paths between links.  The 
        paths between links consider turn restrictions or penalties based on
        the movements in the network. 
        """
        # a dictionary lookup between the link IDs and
        # the graph index for skim and pred
        self.l2i = None
        self.i2l = None
        
        # The N x N matrix of costs between graph links. skim[i,j] gives 
        # the shortest cost from link i to link j along the graph.
        self.linkSkim = None
        
        # The N x N matrix of predecessors, which can be used to reconstruct 
        # the shortest paths. Row i of the predecessor matrix contains information 
        # on the shortest paths from point i: each entry predecessors[i, j] 
        # gives the index of the previous link in the path from point i to point j. 
        # If no path exists between point i and j, then predecessors[i, j] = -9999
        self.linkPred = None


    def readDTANetwork(self, inputDir, filePrefix, logging_dir='C:/temp'):
        """
        Reads the dynameq files to create a network representation. 
        """
        
        # The SanFrancisco network will use feet for vehicle lengths and coordinates, and miles for link lengths
        dta.VehicleType.LENGTH_UNITS= "feet"
        dta.Node.COORDINATE_UNITS   = "feet"
        dta.RoadLink.LENGTH_UNITS   = "miles"

        dta.setupLogging(logging_dir + "/dta.INFO.log", logging_dir+"/dta.DEBUG.log", logToConsole=False)

        scenario = dta.DynameqScenario()
        scenario.read(inputDir, filePrefix) 
        net = dta.DynameqNetwork(scenario)

        net.read(inputDir, filePrefix)
        
        # initialize costs
        dta.Algorithms.ShortestPaths.initiaxblizeEdgeCostsWithFFTT(net)
        dta.Algorithms.ShortestPaths.initialiseMovementCostsWithFFTT(net)        
        
        self.net = net
        

    
    def initializeShortestPathsBetweenNodes(self):
        """
        Calculates the shortest paths between all node pairs and populates
        self.nodeSkim and self.nodePred

        The shortest paths between nodes do not consider turn restrictions
        or turn penalties. 
        """
        
        # STEP 1: create a dictionary lookup between the node IDs and
        # the graph index
        self.n2i = {}
        self.i2n = {}
        
        i = 0
        for node in self.net.iterNodes():   
            node_id = node.getId()
            self.n2i[node_id] = i
            self.i2n[i] = node_id
            i += 1
        num_nodes = i+1
        
        # STEP 2: create a compressed sparse matrix representation of the network, 
        # for use with scipy shortest path algorithms
        anodes = []
        bnodes = []
        costs = []
        for link in self.net.iterRoadLinks():
            a = self.n2i[link.getStartNodeId()]
            b = self.n2i[link.getEndNodeId()]
            cost = 60.0 * link.getFreeFlowTTInMin()
            
            anodes.append(a)
            bnodes.append(b)
            costs.append(cost)
            
        num_links = len(costs)
        
        anodes2 = np.array(anodes)
        bnodes2 = np.array(bnodes)
        costs2  = np.array(costs)
        
        print 'Creating network graph with %i nodes and %i links ' %(num_nodes, num_links)        
        graph = csr_matrix((costs2, (anodes2, bnodes2)), shape=(num_nodes, num_nodes)) 
        
        
        # STEP 3: run the scipy algorithm
        (self.nodeSkim, self.nodePred) = sp.sparse.csgraph.shortest_path(graph, 
                        method='auto', directed=True, return_predecessors=True)
        
        
    def initializeShortestPathsBetweenLinks(self):
        """
        Calculates the shortest paths between all link pairs and populates
        self.linkSkim and self.linkPred

        The paths between links consider turn restrictions or penalties based 
        on the movements in the network. 
        """
        
        # STEP 1: create a dictionary lookup between the node IDs and
        # the graph index
        self.l2i = {}
        self.i2l = {}
        
        i = 0
        for link in self.net.iterRoadLinks():   
            link_id = link.getId()
            self.l2i[link_id] = i
            self.i2l[i] = link_id
            i += 1
        num_links = i+1
        
        # STEP 2: create a compressed sparse matrix representation of the network, 
        # for use with scipy shortest path algorithms
        alinks = []
        blinks = []
        costs = []
        for movement in self.net.iterMovements():
            
            incomingLink = movement.getIncomingLink()
            outgoingLink = movement.getOutgoingLink()
                        
            # only keep if they are both road links
            if (incomingLink.isRoadLink() and outgoingLink.isRoadLink()):
            
                a = self.l2i[incomingLink.getId()]
                b = self.l2i[outgoingLink.getId()]

                # the cost is the travel time on the incoming link
                cost = 60.0 * movement.getFreeFlowTTInMin()
            
                # now we figure out if it is a right or left turn, 
                # and apply a penalty if it is
                if movement.isLeftTurn():
                    cost += self.LEFT_TURN_PENALTY
                elif movement.isRightTurn():
                    cost += self.RIGHT_TURN_PENALTY
                elif movement.isUTurn():
                    cost += self.U_TURN_PENALTY
            
                # and add to my lists
                alinks.append(a)
                blinks.append(b)
                costs.append(cost)
        
        num_movements = len(costs)
        
        alinks2 = np.array(alinks)
        blinks2 = np.array(blinks)
        costs2  = np.array(costs)
        
        print 'Creating network graph with %i links and %i movements ' %(num_links, num_movements)        
        graph = csr_matrix((costs2, (alinks2, blinks2)), shape=(num_links, num_links)) 
        
        
        # STEP 3: run the scipy algorithm
        (self.linkSkim, self.linkPred) = sp.sparse.csgraph.shortest_path(graph, 
                        method='auto', directed=True, return_predecessors=True)
        
    
    def project(self, gps_pos):
        """ (abstract) : takes a GPS position and returns a list of states.
        """
        
        return_tuple = self.findNRoadLinksNearestCoords(gps_pos.x, gps_pos.y, 
            n=self.PROJECT_NUM_LINKS, dist_limit=self.PROJECT_DIST_THRESHOLD)
            
        states = []
        for rt in return_tuple: 
            (roadlink, distance, t) = rt
            offset = t * roadlink.getLengthInCoordinateUnits()
            state = State(roadlink.getId(), offset, distFromGPS=distance)
            states.append(state)
                    
        return states

    
    def findNRoadLinksNearestCoords(self, x, y, n=1, dist_limit = sys.float_info.max):
        """
        Returns the *n* closest road links to the given (*x*, *y*) coordinates.
        
        If *n* = 1, returns a 3-tuple (*roadlink*, *distance*, *t*).  
        The *roadlink* is the closest :py:class:`RoadLink` instance to (*x*, *y*),
        the *distance* is the distance between (*x*, *y*) and the *roadlink*, and 
        *t* is in [0,1] and indicates how far along from the start point and end point
        of the *roadlink* lies the closest point to (*x*, *y*).
        
        If *n* > 1: returns a list of 3-tuples as described above, sorted by the *distance*
        values.
        
        Uses *dist_limit* (if passed) to return only those links within a 
        the specified distance. 
        
        Returns (None, None, None) if none found and *n* = 1, or an empty list for *n* > 1
                
        *x*,*y* and *dist_limit* are  in :py:attr:`Node.COORDINATE_UNITS`
        
        Implementation differs from that used in the dta.Network class in 
        that this one uses rtree for fast spatial indexing.  rtree uses
        a bounding box method, so it may occasionally miss one of the top N
        points, but as long as we have a few in the list, it shoudl be close
        enough. 
        """

        if (self.linkSpatialIndex==None):
            self.initializeSpatialIndex()

        return_tuples       = []
                        
        link_ids = self.linkSpatialIndex.nearest((x, x, y, y), n)

        for link_id in link_ids:
            link = self.net.getLinkForId(link_id)

            (dist, t) = link.getDistanceFromPoint(x,y)
            if dist < dist_limit:
                return_tuples.append( (link, dist, t))

        # sort
        return_tuples = sorted(return_tuples, key=operator.itemgetter(1))
        
        # kick out extras
        while len(return_tuples) > n:
            return_tuples.pop()
                    
        if n==1:
            if len(return_tuples) == 0: 
                return (None, None, None)
            return return_tuples[0]

        return return_tuples
        

    def initializeSpatialIndex(self):
        """
        Creates a spatial index for all links using rtree.  This must be called
        prior to calling findNRoadLinksNearestCoords().
        
        """

        #  The coordinate ordering for all functions are sensitive the the 
        # index’s interleaved data member. If interleaved is False, the 
        # coordinates must be in the form [xmin, xmax, ymin, ymax].
        self.linkSpatialIndex = rtree.index.Index(interleaved=False)

        for link in self.net.iterRoadLinks():
                        
            # draw a box around all shape points when doing this
            coords = link.getCenterLine(wholeLineShapePoints = True)
            x, y = zip(*coords)
            
            self.linkSpatialIndex.insert(link.getId(), (min(x), max(x), min(y), max(y)))
                        
        
    def getPathsUsingNodes(self, s1, s2):
        """ Returns a set of candidate paths between state s1 and state s3.
        Always includes the first and last link. 
        
        Arguments:
        - s1 : a State object
        - s2 : a State object
        """
        
        # if the same link, it's easy
        if (s1.link_id == s2.link_id):
            path = Path(s1, [s1.link_id], s2)    
            return [path]
        
        startNode = self.net.getLinkForId(s1.link_id).getEndNodeId()
        endNode   = self.net.getLinkForId(s2.link_id).getStartNodeId()
        
        # if there is no valid path
        cost = self.nodeSkim[self.n2i[startNode], self.n2i[endNode]]
        if np.isinf(cost):
            return [None]
        
        # sequence of node IDs
        nodeSeq = self.getShortestPathNodeSequence(startNode, endNode)
        
        # convert to a sequence of link IDs
        linkSeq = [s1.link_id]
        for i in range(1,len(nodeSeq)):
            a = nodeSeq[i-1]
            b = nodeSeq[i]        
            link_id = self.net.getLinkForNodeIdPair(a, b).getId()
            linkSeq.append(link_id)
        linkSeq.append(s2.link_id)
        
        # return the path set
        path = Path(s1, linkSeq, s2)        
        return [path]


    def getPaths(self, s1, s2, timeLimit=sys.maxint):
        """ Returns a set of candidate paths between state s1 and state s3.
        Always includes the first and last link. 
        
        Arguments:
        - s1 : a State object
        - s2 : a State object
        - timeLimit: the maximum time allowed for a path, beyond which
                     none is returned
        """
        
        # if the same link, it's easy
        if (s1.link_id == s2.link_id):
            path = Path(s1, [s1.link_id], s2)    
            return [path]
                        
        # sequence of link IDs
        linkSeq = self.getShortestPathLinkSequence(s1.link_id, s2.link_id, timeLimit=timeLimit)
                
        # return the path set
        path = Path(s1, linkSeq, s2)        
        return [path]


    def getShortestPathNodeSequence(self, startNode, endNode):
        """
        returns the sequence of node IDs that define the shortest
        path from the startNode to the endNode. 
        
        Does not consider movement restrictions or turn penalties. 
        
        - startNode: the start node ID (not index)
        - endNode: the end node ID (not index)
        """
        
        # use indices
        start = self.n2i[startNode]
        end = self.n2i[endNode]
        
        # if there is no valid path
        cost = self.nodeSkim[start, end]
        if np.isinf(cost):
            return [None]
        
        # trace the path
        path = []
        j = end
        while (j != start):
            path.append(self.i2n[j])
            j = self.nodePred[start, j]
        path.append(self.i2n[start])
        
        # reverse the list, because we started from the end
        path.reverse()
            
        return path
        

    def getShortestPathLinkSequence(self, startLink, endLink, timeLimit=sys.maxint):
        """
        returns the sequence of link IDs that define the shortest
        path from the startLink to the endLink. 
        
        Considers movement restrictions or turn penalties. 
        
        - startLink: the start link ID (not index)
        - endLink: the end link ID (not index)
        - timeLimit: the maximum time allowed for a path, beyond which
                     none is returned
        """
        
        # use indices
        start = self.l2i[startLink]
        end = self.l2i[endLink]
        
        # if there is no valid path
        if (self.linkSkim[start, end] > timeLimit):
            return []
        
        # trace the path
        path = []
        j = end
        while (j != start):
            path.append(self.i2l[j])
            j = self.linkPred[start, j]
        path.append(self.i2l[start])
        
        # reverse the list, because we started from the end
        path.reverse()
            
        return path

    def getPathsUsingDtaAnywayImplementation(self, s1, s2):
        """ Returns a set of candidate paths between state s1 and state s3.
        Arguments:
        - s1 : a State object
        - s2 : a State object
        
        NOTE: this is slow, so not recommended!
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
                
                # limit possible paths based on a max time diff
                timeDiff = (sc2.time - sc1.time).total_seconds()
                timeLimit = self.TIME_LIMIT_FACTOR * timeDiff
                timeLimit = max(self.TIME_LIMIT_MINIMUM, timeLimit)
                
                # get the paths
                ps = self.getPaths(sc1.states[i1], sc2.states[i2], timeLimit=timeLimit)
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
        
        # get the traversal ratios
        traversalRatios = self.getPathTraversalRatios(path)
        
        # frist get the total time across all links
        tot_tt = 0.0
        for i in range(0,len(path.links)):
            link = self.net.getLinkForId(path.links[i])
            tot_tt += 60.0 * link.getFreeFlowTTInMin() * traversalRatios[i]
                
        return tot_tt
    

    def getPathFreeFlowTTInSecondsWithTurnPenalties(self, path):
        """ Returns the free-flow travel time of the path in seconds, 
        including the cost of turn penalties.
        
        Arguments: a path_inference.structures.Path object
        """
        
        # the skim time includes turn penalties, so start from there
        startLinkId = path.links[0]
        endLinkId = path.links[-1]        
        skimTime = self.linkSkim[self.l2i[startLinkId], self.l2i[endLinkId]]
        
        # adjust the first element, only for the traversal portion of the travel time
        firstOffsetRatio = self.getLinkOffsetRatio(path.start)
        firstLink = self.net.getLinkForId(startLinkId)
        firstLinkTime = 60.0 * firstLink.getFreeFlowTTInMin()
        
        # adjust the last element, only for the traversal portion of the travel time
        lastOffsetRatio = self.getLinkOffsetRatio(path.end)
        lastLink = self.net.getLinkForId(endLinkId)
        lastLinkTime = 60.0 * lastLink.getFreeFlowTTInMin()
        
        tt = (skimTime 
            - (firstOffsetRatio * firstLinkTime) 
            - ((1.0 - lastOffsetRatio) * lastLinkTime))

        return tt


    def getLinkOffsetRatio(self, state):
        """ Returns the offset ratio         
        offset ratio is in [0,1] and indicates how far along from the 
        start point and end point
        """
        link = self.net.getLinkForId(state.link_id)
        dist = link.getLengthInCoordinateUnits()
        ratio = state.offset / dist
        return ratio
    
    
    def getPathTraversalRatios(self, path):
        """ Returns an array of traversal ratios, corresponding to each
        link in the path.  
                
        offset ratio is in [0,1] and indicates the fraction of the link
        that is actually traveled. 
        """
        
        if (len(path.links)==0):
            return []
                
        # start with an array of 1s
        ratios = [1.0] * len(path.links)
        
        # adjust the first element        
        firstOffsetRatio = self.getLinkOffsetRatio(path.start)
        ratios[0] = ratios[0] - firstOffsetRatio
        
        # adjust the last element
        lastOffsetRatio = self.getLinkOffsetRatio(path.end)
        ratios[len(path.links)-1] = ratios[len(path.links)-1] - (1.0-lastOffsetRatio)
        
        return ratios        
        

    def allocatePathTravelTimeToLinks(self, path, start_time, end_time):
        """ Returns three lists for: 
            
            (link_id, traversalRatio, travelTime)
            
            where traversalRatio is the fraction of the link actually traversed
            and travelTime is in seconds and the travel time to go across
            that fraction of the link.
            
            Note that for the first and last links, only a portion of
            the link may be traversed.  
        
        Arguments: a path_inference.structures.Path object
                   a datetime object for the start time
                   a datetime object for the end time
        """
                
        # get the traversal ratios
        traversalRatios = self.getPathTraversalRatios(path)
        
        # get the totals
        tot_tt = (end_time - start_time).total_seconds()
        tot_ff_time = self.getPathFreeFlowTTInSeconds(path)
        
        # allocate the travel time
        link_tt = []
        for i in range(0,len(path.links)):
            
            # if the vehicle is stopped, or effectively stopped
            # then allocate the travel time equally across all links
            if (tot_ff_time < 0.1): 
                tt = tot_tt * (1.0/len(path.links))

            # othwerwise make it proportional to the free-flow times
            else: 
                link = self.net.getLinkForId(path.links[i])
                ff_time = 60.0 * link.getFreeFlowTTInMin() * traversalRatios[i]
                tt = tot_tt * (ff_time / tot_ff_time)
                
            link_tt.append(tt)        
        
        return (path.links, traversalRatios, link_tt)
        
    
    def getRoadLinkDataFrame(self):
        """
        Returns a dataframe with one record for each road link, 
        containing key link attributes.  
        
        """

        data = []
        for link in self.net.iterRoadLinks():
            
            row = {}
            
            # ID fields
            row['ID']        = link.getId()
            row['ANODE']     = link.getStartNode().getId()
            row['BNODE']     = link.getEndNode().getId()
            
            # coordinates (can be more than two)
            coords = link.getCenterLine(wholeLineShapePoints = True)
            x, y = zip(*coords)
            row['X'] = x
            row['Y'] = y

            # attributes
            row['TYPE']     = 'RoadLink'
            row['LABEL']    = link.getLabel()
            row['FACTYPE']  = link.getFacilityType()
            row['LANES']    = link.getNumLanes()
            row['DIR']      = link.getDirection()
            row['LENGTH']   = link.getLength()
            row['FFSPEED']  = link.getFreeFlowSpeedInMPH()
            row['FFTIME']   = 60.0 * link.getFreeFlowTTInMin()

            data.append(row)
        
        df = pd.DataFrame(data)
        
        return df
        
