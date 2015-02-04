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

from path_inference.path_builder import PathBuilder

class SFPathBuilder(PathBuilder):
  """ Creates candidate paths between states.
  
  Extends PathBuilder to actually create the paths.  
  """
  
  def getPaths(self, s1, s2):
    """ Returns a set of candidate paths between state s1 and state s3.
    Arguments:
    - s1 : a State object
    - s2 : a State object
    """
    print 'In Gregs Method'
    raise NotImplementedError()
