"""
clearquest.merge: module for merging ClearQuest databases
"""

#===============================================================================
# Imports
#===============================================================================

import os
import sys
import sql

from constants import *
from util import connectStringToMap
from task import Task, TaskManager

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
def findSql(name, *args, **kwds):
    try:
        session = kwds['session']
        del kwds['session']
    except:
        session = sys._getframe().f_back.f_locals['session']
    return sql._findSql(session, 'merge', name, *args, **kwds)

def createDbIdMaps(session):
    auxTable = 'merge_aux_dbids'
    reqTable = 'merge_req_dbids'
    auxSql = findSql('selectAuxDbIds') 
    reqSql = findSql('selectReqDbIds')
    
    auxSelect = '\nUNION\n'.join([
        auxSql % (e.GetName(), e.GetDbName()) for e in [
            session.GetEntityDef(n) for n in session.GetEntityDefNames()
        ] if e.GetType() == EntityType.Stateless
    ])
    
    reqSelect = '\nUNION\n'.join([
        reqSql % e.GetDbName() for e in [
            session.GetEntityDef(n) for n in session.GetEntityDefNames()
        ] if e.GetType() == EntityType.Stateful
    ])
    
    session.db().execute(findSql('createDbIdMaps', **locals()))

def insertEntity(sourceSession, destSession, entityDef, start=0, end=0):
    sm = connectStringToMap(sourceSession.connectString())
    dm = connectStringToMap(destSession.connectString())
    
    entityDbName = entityDef.GetDbName()
    srcTable =  '%s.%s.%s' % (sm['DATABASE'], sm['UID'], entityDbName)
    dstTable =  '%s.%s.%s' % (dm['DATABASE'], dm['UID'], entityDbName)
    session = destSession
    dbFieldNames = ',\n'.join([
        '    %s' % entityDef.getFieldDbName(f)
            for f in entityDef.GetFieldDefNames()
                if entityDef.GetFieldDefType(f) in FieldType.dbScalarTypes
    ])
    sql = findSql('insertEntity', **locals())
    return sql
    #print sql
    #session.db().execute(sql)
    

#===============================================================================
# Classes
#=============================================================================== 

class MergeTask(Task):
    def __init__(self, manager):
        Task.__init__(self, manager)
        self.sourceSession=manager.getSourceSession(self.getSessionClassType())

    def getSessionClassType(self):
        return SessionClassType.User

class DisableEntityIndexesTask(MergeTask):
    def __init__(self, manager, entityDefName):
        MergeTask.__init__(self, manager)
    
    def run(self):
        s = self.destSession
        [ s.GetEntityDef(n).disableAllIndexes() for n in s.GetEntityDefNames() ]
        
class EnableEntityIndexesTask(MergeTask):
    def __init__(self, manager, entityDefName):
        MergeTask.__init__(self, manager)
    
    def run(self):
        s = self.destSession
        [ s.GetEntityDef(n).enableAllIndexes() for n in s.GetEntityDefNames() ]
        

class MergeEntityTask(MergeTask):
    def __init__(self, manager, entityDefName, start, end):
        MergeTask.__init__(self, manager)
        self.entityDefName = entityDefName
        self.entityDef = self.destSession.GetEntityDef(entityDefName)
        self.entityDbName = self.entityDef.GetDbName()
        self.start = start
        self.end = end
    
    def run(self):
        cb = self.cb
        sql = 'SELECT COUNT(*) FROM %s WHERE dbid <> 0' % self.entityDbName
        cb.expected = self.sourceSession.db().selectSingle(sql)