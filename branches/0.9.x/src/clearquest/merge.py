"""
clearquest.merge: module for merging ClearQuest databases
"""

#===============================================================================
# Imports
#===============================================================================

import sys

from clearquest import db
from clearquest.task import Task, TaskManager
from clearquest.util import connectStringToMap
from clearquest.constants import EntityType, FieldType, SessionClassType

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
    return db._findSql(session, 'merge', name, *args, **kwds)

@db.execute
def createDbIdMaps(session):
    auxSql = findSql('selectAuxDbIds') 
    reqSql = findSql('selectReqDbIds')
    return {
        'auxTable'  :   'merge_aux_dbids',
        'reqTable'  :   'merge_req_dbids',
        'auxSelect' :   '\nUNION\n'.join([
                            auxSql % (e.GetName(), e.GetDbName()) 
                                for e in session.getStatelessEntityDefs() ]),
        'reqSelect' :   '\nUNION\n'.join([
                            reqSql % e.GetDbName()
                                for e in session.GetStatefulEntityDefs() ]),
    }

@db.execute
def deleteAllStatefulEntitiesWhereIdStartsWith(session, prefix):
    return {
        'prefix': prefix,
        'tableNames': [ e.GetDbName() for e in session.getStatefulEntityDefs() ]
    }

    
def insertEntity(sourceSession, destSession, entityDefName):
    entityDef = destSession.GetEntityDef(entityDefName)
    
    columns = [
        entityDef.getFieldDbName(f)
            for f in entityDef.GetFieldDefNames()
                if entityDef.GetFieldDefType(f) in [
                    FieldType.ShortString,
                    FieldType.MultilineString,
                    FieldType.Integer,
                    FieldType.DateTime,
                    FieldType.State,                                                    
                    FieldType.Reference,
                ]
    ]
    
    entityDbName = entityDef.GetDbName()
    sm = connectStringToMap(sourceSession.connectString())
    dm = connectStringToMap(destSession.connectString())
    srcTable = '%s.%s.%s' % (sm['DATABASE'], sm['UID'], entityDbName)
    dstTable = '%s.%s.%s' % (dm['DATABASE'], dm['UID'], entityDbName)
    
    where = 'WHERE src.dbid <> 0'
    dbidColumn = 'src.dbid'
    collate = 'COLLATE SQL_Latin1_General_CP1_CI_AS'
    dstSql = entityDef.getUniqueKey()._buildSql(select='t1.dbid')[:-1]
    srcSql = sourceSession.GetEntityDef(entityDefName)      \
                          .getUniqueKey()                   \
                          ._buildSql(where='t1.dbid')[:-1]  \
                          .replace('FROM', '%s FROM' % collate)
                          
    if entityDef.GetType() == EntityType.Stateless:
        where += ' AND NOT EXISTS (%s(%ssrc.dbid))' % (dstSql, srcSql)
    else:
        dbidColumn = '(CASE WHEN EXISTS (%s) THEN (%s) ELSE src.dbid END)' % (\
            'SELECT 1 FROM %s WHERE dbid = src.dbid' % dstTable,
            'SELECT 1+(SELECT MAX(dbid) FROM %s) FROM dbglobal' % dstTable
        )
        
    dstColumns = columns + [ '__old_dbid', 'dbid' ]
    srcColumns = [ 'src.%s' % c for c in columns ] + [ 'src.dbid', dbidColumn ]
    
    kwds = {
        'where'    : where,
        'session'  : destSession,
        'srcTable' : srcTable,
        'dstTable' : dstTable,
        'srcColumns' : ',\n    '.join(srcColumns),
        'dstColumns' : ',\n    '.join(dstColumns),
        'dbidColumn' : dbidColumn,
    }
    sql = findSql('insertEntity', **kwds)
    return sql
    #print sql
    #session.db().execute(sql)
    

#===============================================================================
# Classes
#=============================================================================== 

class MergeManager(TaskManager):
    def __init__(self, manager):
        Task.__init__(self, manager)
        self.defaultConfigSection = 'DEFAULT' \
            if len(sys.argv) == 1             \
            else sys.argv[1]
            
#        self.tasks = [
#            self.addOldDbIdColumnsToEntities,
#            self.takeSnapshot,
#            self.disableAllIndexes,
#            self.mergeUsersAndGroups,
#            self.mergeStatelessEntities,
#            self.insertStatefulEntities,
#            self.merge
#            
#            
#            
#            self.fixAllReferences
#            
#            
#            
#            
#            
#                      
#        ]
            
    def getDefaultConfigSection(self):
        return self.defaultConfigSection

    def run(self):
        
        self.mergeNonExistingStatelessEntities()
        pass
        

class MergeTask(Task):
    def __init__(self, manager):
        Task.__init__(self, manager)
        self.sourceSession=manager.getSourceSession(SessionClassType.User)

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