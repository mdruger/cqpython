"""
clearquest.util: module for any miscellaneous utilities (methods, classes etc)
"""

#===============================================================================
# Imports
#===============================================================================

import os
import sys
import time
import inspect
import traceback
import api
import callback

from functools import wraps
from itertools import repeat
from os.path import basename, dirname
from ConfigParser import ConfigParser, NoOptionError
from lxml.etree import XML

#===============================================================================
# Globals
#===============================================================================
__rcsid__ = '$Id$'
__rcsurl__ = '$URL$'
__copyright__ = 'Copyright 2008 OnResolve Ltd'

#===============================================================================
# Decorators
#===============================================================================

#===============================================================================
# Helper Methods
#===============================================================================

def joinPath(*args):
    return os.path.normpath(os.path.join(*args))

def connectStringToMap(connectString):
    return dict([ v.split('=') for v in connectString.split(';') ])

def spliceWork(dataOrSize, nchunks):
    if type(dataOrSize) in (list, tuple):
        max = len(dataOrSize)
    else:
        max = dataOrSize
    size = max / n
    results = []
    results.append((0, size))
    for i in range(1, nchunks-1):
        results.append((i*size, (i+1)*size))
    results.append(((nchunks-1)*size, max-1))
    return results

#===============================================================================
# Classes
#===============================================================================

class Dict(dict):
    def __init__(self, **kwds):
        dict.__init__(self)
        self.__dict__.update(**kwds)
    def __getattr__(self, name):
        return self.__getitem__(name)
    def __setattr__(self, name, value):
        return self.__setitem__(name, value)    

