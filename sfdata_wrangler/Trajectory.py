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

from path_inference import utils
from path_inference.structures import LatLng, StateCollection
from path_inference.learning_traj import LearningTrajectory

from PathBuilder import SFPathBuilder
from Projector import SFPointProjector
    

# This is used as a scoring function, where each possible state is given
# a score based on the distance from that state to the recorded GPS
# position.  It is a maximization problem, so the score must be negative. 
# 
# It returns an array with two elements, the first element being the
# pathscore and the second being the pointscore.  Since this is for points, 
# the pathscore is always zero.  
#
# The pointscore is based on the distance from the candidate state to the 
# recorded GPS position. 
def point_feature_vector(sc):
  """ The feature vector of a point.
  """
  return [[0, utils.distance(sc.gps_pos, s.gps_pos)] for s in sc.states]


# This is used as a scoring function, where the path is given a score
# based on the square of the difference in travel time calculated from 
# the links versus between the GPS recordings. It is a maximization problem, 
# so the score must be negative. 
# 
# It returns an array with two elements, the first element being the
# pathscore and the second being the pointscore.  Since this is for paths, 
# the pointscore is always zero.  
# 
# Here, the scoring is based on the sum of the absolute difference in 
# travel time, and the travel time in excess of what is observed.  In this
# way, we double-penalize paths that look too long, getting shorter paths. 
def path_feature_vector(path, tt):
  """ The feature vector of a path.
  """  
  path_tt = path.getTravelTimeInSeconds()
  score = abs(path_tt - tt) + max(path_tt - tt, 0)
  
  return [score, 0]


class Trajectory():
    """ 
    Class to represent a vehicle trajectory through a network. 
    """
    

    def __init__(self, df):
        """
        Constructor.             
        """   

        # TODO not sure if this should go here...
        path_builder = SFPathBuilder()
        projector = SFPointProjector()

        
        # there is one point for each GPS observations
        # the states are collection of possible locations in the network
        self.points = []                

        # there is one set of paths betweeen each pair of GPS observations
        # for each, there is a collection of possible paths
        self.paths = []
        
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

        # STEP 1: Create the points
        traveltime = []
        for i, row in df.iterrows():
            traveltime = row['seconds']
            position = LatLng(row['latitude'], row['longitude'])         
            states = projector.project(position)
            sc = StateCollection(row['cab_id'], states, position, row['time'])
            self.points.append(sc)
        
        # STEP 2: Create the candidate paths between each point
        #         and fill up the features while we're at it
        self.features.append(point_feature_vector(self.points[0]))
        
        for i in range(1, len(self.points)):
            (trans1, ps, trans2) = \
                path_builder.getPathsBetweenCollections(self.points[i-1], self.points[i])
            
            # transitions and paths
            self.transitions.append(trans1)
            self.paths.append(ps)
            self.transitions.append(trans2)
            
            # features are used for scoring
            paths_features = []
            for path_ in ps:
                paths_features.append(path_feature_vector(path_, traveltime[i]))
            self.features.append(paths_features)
            self.features.append(point_feature_vector(self.points[i]))
            

        # a LearningTrajectory is the basic data structure that stores the 
        # features (scores candidate states candidate paths), and transitions 
        # (indices to look up those states or paths).  
        self.learningTrajectory = LearningTrajectory(self.features, self.transitions)

        