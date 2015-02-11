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
import HwyNetwork
import numpy as np
from mm.path_inference.structures import StateCollection, Position
from mm.path_inference.learning_traj import LearningTrajectory
from mm.path_inference.learning_traj_viterbi import TrajectoryViterbi1
    


def point_feature_vector(sc):
    """ The feature vector of a point.

    This is used as a scoring function, where each possible state is given
    a score based on the distance from that state to the recorded GPS
    position.  It is a maximization problem, so the score must be negative. 
         
    It returns an array with two elements, the first element being the
    pathscore and the second being the pointscore.  Since this is for points, 
    the pathscore is always zero.  There is one element for each state 
    in the state collection
                
    The pointscore is based on the distance from the candidate state to the 
    recorded GPS position. 

    """
    point_features = []
    for s in sc.states:
        score = [0, -s.distFromGPS]
        point_features.append(score)
    return point_features
    
    

def path_feature_vector(hwynet, path, tt):
    """ The feature vector of a path.

    This is used as a scoring function, where the path is given a score
    based on the square of the difference in travel time calculated from 
    the links versus between the GPS recordings. It is a maximization problem, 
    so the score must be negative. 
         
    It returns an array with two elements, the first element being the
    pathscore and the second being the pointscore.  Since this is for paths, 
    the pointscore is always zero.  
         
    Here, the scoring is based on the sum of the absolute difference in 
    travel time, and the travel time in excess of what is observed.  In this
    way, we double-penalize paths that look too long, getting shorter paths. 

    """  
    
    if (path==None):
        return [-sys.maxint, 0]
        
    else: 
        path_tt = hwynet.getPathFreeFlowTTInSeconds(path)
        score = -1.0 * (abs(path_tt - tt) + max(path_tt - tt, 0))        
        return [score, 0]
    

class Trajectory():
    """ 
    Class to represent a vehicle trajectory through a network. 
    """

    # THETA is a way to weight the relative value given to the 
    # path score versus the point score when selecting the most
    # likely trajectory.  Its format is n.parray([pathweight, pointweight])
    # These weights can be tuned to achieve good results. 
    THETA = np.array([1.0, 1.0])
    
    
    def __init__(self, hwynet, df):
        """
        Constructor. 
        hwynet - a HwyNetwork for projecting and building paths
        df - a dataframe with GPS points for this trajectory  
        """   
        
        # there is one point for each GPS observations
        # the states are collection of possible locations in the network
        self.candidatePoints = []                

        # there is one set of paths betweeen each pair of GPS observations
        # for each, there is a collection of possible paths
        self.candidatePaths = []

        # the observed travel times corresponding to these paths
        self.traveltimes = []
        
        # features is a scoring method for each candidate point or path
        #
        # point_feature_vector contains [pathscore, pointscore] for each 
        # candidate state.  
        # 
        # path_feature_vector contains [pathscore, pointscore] for each 
        # candidate path. 
        # 
        # In total, the features are an alternating sequence of points and paths, 
        # starting and ending with points. 
        self.features = []

        # The transitions go between each element (point or path) in the trajectory, 
        # so there are len(features) - 1 transitions.  
        # For points, a transition is (index of candidate state, index of candidate path)
        # For paths, a transition is (index of candidate path, index of candidate state)
        self.transitions = []
           
        # The indexes of the most likely elements of the trajectory
        # Point indexes and path indexes are interleaved.
        self.most_likely_indices = None


        # STEP 1: Create the points
        firstRow = True
        for i, row in df.iterrows():
            position = Position(row['x'], row['y'])         
            states = hwynet.project(position)
            
            # if point is not near any links, get out of here.
            # don't just keep going because that could cause a discontinutity
            if (len(states)==0):
                break
            
            sc = StateCollection(row['cab_id'], states, position, row['time'])
            self.candidatePoints.append(sc)
            
            # travel times between the points
            if (not firstRow): 
                self.traveltimes.append(row['seconds'])
            firstRow = False
            
        # STEP 2: Check that we're not dealing with an emtpy set
        if (len(self.candidatePoints)==0):
            return
                
        # STEP 3: Create the candidate paths between each point
        #         and fill up the features while we're at it
        point_scores = point_feature_vector(self.candidatePoints[0]) 
        self.features.append(point_scores)
        
        for i in range(1, len(self.candidatePoints)):
            (trans1, ps, trans2) = \
                hwynet.getPathsBetweenCollections(self.candidatePoints[i-1], self.candidatePoints[i])
            
            # transitions and paths
            self.transitions.append(trans1)
            self.candidatePaths.append(ps)
            self.transitions.append(trans2)
            
            # features are used for scoring
            paths_features = []
            for path_ in ps:
                path_scores = path_feature_vector(hwynet, path_, self.traveltimes[i-1])
                paths_features.append(path_scores)
            self.features.append(paths_features)
            
            point_scores = point_feature_vector(self.candidatePoints[i])
            self.features.append(point_scores)
        

    def calculateMostLikely(self):
        """ Calculates the indices of the most likely trajectory.
        
        The result alternates between point indices and path indices
        and correspond to the candidate points and candidate paths. 
        """
        
        # a LearningTrajectory is the basic data structure that stores the 
        # features (scores candidate states candidate paths), and transitions 
        # (indices to look up those states or paths). 
        traj = LearningTrajectory(self.features, self.transitions)

        # The viterbi is a specific algorithm that calculates the most likely
        # states and most likley paths.  The key output are the indices noted 
        # below, which can be used to look up the specific candidate states
        # and candidate paths (although those must be stored externally.  
        # There is one index for each feature. 
        viterbi = TrajectoryViterbi1(traj, self.THETA)
        try: 
            viterbi.computeAssignments()
        except (ValueError):
            for f in self.features:
                print f
            for t in self.transitions:
                print t
            print self.THETA
            raise 

        # The indexes of the most likely elements of the trajectory
        # Point indexes and path indexes are interleaved.
        self.most_likely_indices = viterbi.assignments
    

    def getMostLikelyPaths(self):
        """ Returns an array of the most likely paths to be traversed.  
                
        """
        
        if self.most_likely_indices == None:
            raise RuntimeError('Need to calculate most likely indices!')         
        
        elements = []
        for i in range(0, len(self.most_likely_indices)):
            
            # it is only a path if i is odd, otherwise its a state collection
            if ((i%2) == 1): 
                j = (i-1) / 2
                path = self.candidatePaths[j][self.most_likely_indices[i]]
                elements.append(path)
        
        return elements


    def getPathStartEndTimes(self):
        """ Returns the starting and ending times for each path
        in the trajectory.  
                
        """        
        times = []
        for i in range(1, len(self.candidatePoints)):
            startTime = self.candidatePoints[i-1].time
            endTime = self.candidatePoints[i].time
            times.append((startTime, endTime))
        
        return times


        