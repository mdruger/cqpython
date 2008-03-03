"""
clearquest.merge: module for merging ClearQuest databases
"""

#===============================================================================
# Imports
#===============================================================================

import sys
import itertools

from pywintypes import com_error

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
        try:
            session = sys._getframe().f_back.f_locals['session']
        except:
            session = sys._getframe().f_back.f_locals['destSession']
    return db._findSql(session, 'merge', name, *args, **kwds)

@db.getSql
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
                                for e in session.getStatefulEntityDefs() ]),
    }

@db.execute
def deleteAllStatefulEntitiesWhereIdStartsWith(session, prefix):
    return {
        'prefix': prefix,
        'tableNames': [ e.GetDbName() for e in session.getStatefulEntityDefs() ]
    }
   
@db.getSql
def insertEntity(destSession, sourceSession, entityDefName):
    entityDef = destSession.GetEntityDef(entityDefName)
    uniqueKey = entityDef.getUniqueKey()
    requiresCollation = False
    destCollation = destSession.getCollation()
    sourceCollation = sourceSession.getCollation()
    if destCollation != sourceCollation:
        requiresCollation = bool(uniqueKey._info()['text'])
    
    columns = [
        entityDef.getFieldDbName(f)
            for f in entityDef.GetFieldDefNames()
                if entityDef.GetFieldDefType(f) in [
                    FieldType.ShortString,
                    FieldType.MultilineString,
                    FieldType.Integer,
                    FieldType.DateTime,
                    FieldType.State,
                    FieldType.Id,
                ]
    ]
    
    entityDbName = entityDef.GetDbName()
    srcPrefix = sourceSession.getTablePrefix().upper()
    dstPrefix = destSession.getTablePrefix().upper()
    srcTable = '.'.join((srcPrefix, entityDbName))
    dstTable = '.'.join((dstPrefix, entityDbName))
    
    ddb = destSession.db()
    addOldDbId = False
    if not '__old_dbid' in [ c[0] for c in ddb.columns(entityDef.GetDbName()) ]:
        addOldDbId = True
    
    where = 'WHERE src.dbid <> 0'
    dbidColumn = 'src.dbid'
    dstSql = uniqueKey._buildSql(select='t1.dbid')[:-1]
    srcSql = sourceSession.GetEntityDef(entityDefName)      \
                          .getUniqueKey()                   \
                          ._buildSql(where='t1.dbid')[:-1]
    
    if requiresCollation:
        srcSql = srcSql.replace('FROM', 'COLLATE %s FROM' % destCollation)
                          
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
        'srcTable' : srcTable,
        'dstTable' : dstTable,
        'srcColumns' : ',\n    '.join(srcColumns),
        'dstColumns' : ',\n    '.join(dstColumns),
        'dbidColumn' : dbidColumn,
        'addOldDbId' : addOldDbId,
        'entityDbName': entityDbName,
        'destSession': destSession,
        'sourceSession': sourceSession,
    }
    return kwds

def bulkCopy(destSession, sourceSessions, dbidOffsets):
              
    dbidOffsets = list(dbidOffsets)
    preCopySql  = []
    bulkCopySql = []
    postCopySql = []
    enableIndexesSql = []
    disableIndexesSql = []
    
    straightCopyTargets = (
        'history',
        'attachments',
        'attachments_blob',
        'parent_child_links',
    )
    targets = list(straightCopyTargets) + [
        e.GetName() for e in destSession.getAllEntityDefs()
            if e.GetName() not in straightCopyTargets
    ]
    dstReplicaId = destSession.getReplicaId()
    dstPrefix = destSession.getTablePrefix()
    dstDb = destSession.db()
    
    (userEntityDefId, groupsFieldDefId) =                       \
        dstDb.selectAll(                                        \
            "SELECT e.id, f.id FROM fielddef f, entitydef e "   \
            "WHERE e.name = 'users' AND f.name = 'groups' AND " \
            "f.entitydef_id = e.id"                             \
        )[0]
    
    preCopySql.append(findSql('bulkCopy.setPreCopyDatabaseOptions', **{
        'destSession'  : destSession
    }))
    
    sessionCounter = itertools.count(1)
    for sourceSession in sourceSessions:
        sessionCount = sessionCounter.next()
        emptyDb = False
        firstSession = False
        if sessionCount == 1:
            firstSession = True
            # Crude check to see if 'destSession' is pointing to an empty (i.e.
            # newly created) ClearQuest database by seeing whether or not the
            # default entity has any records.
            if destSession.GetDefaultEntityDef().getCount() == 0:
                emptyDb = True
            else:
                # Assume that 'destSession' has been restored from another db
                # backup.  Update dbglobal and ratl_replicas such that they
                # reflect the destination database's intended values.
                preCopySql.append(                                             \
                    "UPDATE %s.dbglobal SET site_name = '%s'" %                \
                    (dstPrefix, destSession._databaseName)
                )
                preCopySql.append(                                             \
                    "UPDATE %s.ratl_replicas SET family = '%s' "               \
                    "WHERE dbid <> 0" %                                        \
                        (dstPrefix, destSession._databaseName)
                )
                for entityDef in destSession.getAllEntityDefs():
                    try:
                        if not entityDef.GetFieldDefType('id') == FieldType.Id:
                            continue
                    except com_error:
                        continue
                    else:
                        preCopySql.append(\
                            findSql('bulkCopy.updateIds', **{
                                'dstTable'  : entityDef.GetDbName(),
                                'dstPrefix' : dstPrefix,
                                'replicaId' : dstReplicaId,
                            })
                        )
                    
        if not emptyDb:
            dbidOffset = dbidOffsets.pop(0)
            
        sourceReplicaId = str(sourceSession.getReplicaId())
        
        for target in targets:
            if target in straightCopyTargets:
                straightCopy = True
                tableName = target
                entityDef = None
                entityDefName = None
            else:
                straightCopy = False
                entityDef = destSession.GetEntityDef(target)
                entityDefName = entityDef.GetName()
                tableName = entityDef.GetDbName()
            
            dstTable = '.'.join((dstPrefix, tableName))
            srcPrefix = sourceSession.getTablePrefix()
            srcTable = '.'.join((srcPrefix, tableName))
            
            if firstSession:
                disableIndexesSql += [
                    'ALTER INDEX %s ON %s DISABLE' % (index, dstTable)
                        for index in dstDb.getIndexes(tableName)
                ]
                enableIndexesSql += [
                    'ALTER INDEX %s ON %s REBUILD' % (index, dstTable)
                        for index in dstDb.getIndexes(tableName)
                ]
                
            exclude = (
                'dbid',
                'ratl_keysite',
                'ratl_mastership',
                'lock_version', 
                'locked_by',
            )
            columns = [
                (c[3], 'src.%s' % c[3])
                    for c in dstDb.cursor().columns(table=tableName).fetchall()
                        if c[3] not in exclude
            ]
            
            if target in ('attachments_blob', 'parent_child_links'):
                orderBy = ''
                dbidColumn = ''
                where = ''
                if target == 'parent_child_links':
                    where = "WHERE NOT (src.parent_entitydef_id = %d AND "     \
                            "src.child_entitydef_id = %d AND "                 \
                            "src.parent_fielddef_id = %d AND "                 \
                            "src.child_fielddef_id = 0 AND "                   \
                            "src.link_type_enum = 1)" %                        \
                                (userEntityDefId,                              \
                                 userEntityDefId,                              \
                                 groupsFieldDefId)
                    
            else:
                where = 'WHERE src.dbid <> 0'
                #orderBy = 'ORDER BY src.dbid ASC'
                
                orderBy = ''
                if not emptyDb:
                    dbidColumn = '(src.dbid + %s)' % dbidOffset
                else:
                    dbidColumn = 'src.dbid'
                if not straightCopy and \
                       entityDef.GetType()==EntityType.Stateless:
                    where += ' AND NOT EXISTS (%s)' % \
                             entityDef                \
                                .getUniqueKey()       \
                                ._lookupDbIdFromForeignSessionSql('src.dbid',
                                                                  sourceSession)
                
            columns += [ ('ratl_mastership', sourceReplicaId) ]
            if target in ('attachments_blob', 'parent_child_links'):
                addReplicaColumn = True
            else:
                addReplicaColumn = False
                columns += [ ('dbid', dbidColumn) ]
            
            dstColumns = [ c[0] for c in columns ]
            srcColumns = [ c[1] for c in columns ]
            
            kwds = {
                'where'    : where,
                'orderBy'  : orderBy,
                'srcTable' : srcTable,
                'dstTable' : dstTable,
                'tableName': tableName,
                'srcColumns' : ',\n    '.join(srcColumns),
                'dstColumns' : ',\n    '.join(dstColumns),
                'addReplicaColumn' : addReplicaColumn,
            }
            bulkCopySql.append(findSql('bulkCopy', **kwds))
            
            if straightCopy or entityDefName in ('users', 'groups'):
                continue
            
            # If we get here, we've generated the SQL necessary for bringing the
            # entity/table over via a direct insert.  Our next step requires us
            # to generate SQL for updating the reference fields present on the
            # table, then reference lists in parent_child_links, then entries
            # in attachments_blob that are pointing to us, then history table
            # entries.
            
            updates = dict()
            for f in entityDef.getReferenceFieldNames():
                if f in ('ratl_keysite', 'ratl_mastership'):
                    continue
                
                r = entityDef.GetFieldReferenceEntityDef(f)
                if emptyDb and r.GetName() not in ('users', 'groups'):
                    continue
            
                column = entityDef.getFieldDbName(f)
                if r.GetType() == EntityType.Stateful:
                    newDbId = '%s + %s' % (column, dbidOffset)
                else:
                    newDbId = r.getUniqueKey() \
                               ._lookupDbIdFromForeignSessionSql(
                                    column, sourceSession)
                               
                updates[column] = '(CASE WHEN %s = 0 THEN 0 ELSE (%s) END)' %  \
                                   (column, newDbId)
                                   
            if updates:
                postCopySql.append(                                 \
                    "UPDATE %s SET %s WHERE ratl_mastership = %s" % \
                    (dstTable,
                     ', '.join(['%s = %s' % i for i in updates.items() ]),
                     sourceReplicaId)
                )
            
            # If we're dealing with a stateful entitydef, add another SQL stmt
            # to update the id to match the new dbid.
            if entityDef.GetType() == EntityType.Stateful:
                postCopySql.append(\
                    findSql('bulkCopy.updateIdsFromDbIds', **{
                        'dstTable'          : dstTable,
                        'dstPrefix'         : dstPrefix,
                        'sourceReplicaId'   : sourceReplicaId,
                    })
                )
            
            # Repeat for reference lists.  We need to generate two types of SQL
            # statements: one that takes care of forward references (parent_-
            # entitydef_id will match our entitydef id) and back references 
            # (child_entitydef_id will match our entitydef id).
            
            updates = dict()
            kwds = {
                'dstTable'          : dstTable,
                'dstPrefix'         : dstPrefix,
                'entityDefName'     : entityDefName,
                'sourceReplicaId'   : sourceReplicaId,
            }
            referenceListFields = [
                ('parent', entityDef.getReferenceListFieldNames()),
                ('child',  entityDef.getBackReferenceListFieldNames()),
            ]
            for (scope, fields) in referenceListFields:
                kwds['scope'] = scope
                for f in fields:
                    
                    r = entityDef.GetFieldReferenceEntityDef(f)
                    if emptyDb and r.GetName() not in ('users', 'groups'):
                        continue
                
                    column = '%s_dbid' % scope
                    if r.GetType() == EntityType.Stateful:
                        newDbId = '%s + %s' % (column, dbidOffset)
                    else:
                        newDbId = r.getUniqueKey() \
                                   ._lookupDbIdFromForeignSessionSql(
                                        column, sourceSession)
                                   
                    kwds['newDbId'] = newDbId
                    kwds['fieldDefName'] = f
                    postCopySql.append(\
                        findSql('bulkCopy.updateParentChildLinks', **kwds),
                    )
            
            # If we're an empty database, we don't need to update any history or
            # attachment fields, so continue at this point.
            if emptyDb:
                continue
            
            # The 'bulkCopy.updateHistory' SQL actually issues two statements;
            # one UPDATE that takes care of updating the entity_dbid for the
            # imported entity, then a DELETE to get rid of any history rows that
            # were brought over for entities that aren't present in the final db
            # (which would be the case for stateless entities that already had
            # an existing entity in the database matching their unique display
            # name).
            kwds = {
                'dstTable'  	: dstTable,
                'dstPrefix' 	: dstPrefix,
                'dbidOffset'    : dbidOffset,
                'entityDefName' : entityDefName,
                'sourceReplicaId': sourceReplicaId,
            }
            postCopySql.append(findSql('bulkCopy.updateHistory', **kwds))
            
            # And finally, update attachments & attachments_blob.
            #kwds['session'] = destSession
            postCopySql.append(findSql('bulkCopy.updateAttachments', **kwds))
            
            
        if not emptyDb:
            postCopySql.append("DELETE %s.history WHERE ratl_mastership = %s" %\
                               (dstPrefix, sourceReplicaId))
            postCopySql.append(                                                \
                "DELETE %s.parent_child_links WHERE ratl_mastership = %s" %    \
                    (dstPrefix, sourceReplicaId)
            )
            postCopySql.append(\
                findSql('bulkCopy.updateAttachmentBlobs', **{
                    'dstTable'  : dstTable,
                    'dstPrefix' : dstPrefix,
                    'dbidOffset': dbidOffset,
                    'sourceReplicaId': sourceReplicaId,
                })
            )
            allEntityDefs = destSession.getAllEntityDefs()
            for e in allEntityDefs:
                postCopySql.append(                                            \
                    "UPDATE %s.%s SET ratl_mastership = %d WHERE dbid <> 0 "   \
                    "AND ratl_mastership <> %d" %                              \
                        (dstPrefix,
                         e.GetDbName(),                 
                         dstReplicaId,
                         dstReplicaId)
                )
    
    postCopySql.append(                                                        \
        "UPDATE %s.dbglobal SET next_request_id = next_request_id + %s, "      \
        "next_aux_id = next_aux_id + %s" %                                     \
            (dstPrefix, dbidOffsets[-1], dbidOffsets[-1])
    )
    
    postCopySql.append(\
        findSql('bulkCopy.setPostCopyDatabaseOptions', **{
            'destSession' : destSession,
        })
    )
    
    return (disableIndexesSql,
            preCopySql,
            bulkCopySql,
            postCopySql,
            enableIndexesSql)

def bulkCopyBackup(destSession, sourceSessions, dbidOffsets):
    targets = [ e.GetName() for e in destSession.getAllEntityDefs() ] + \
              [ 'attachments_blob', 'parent_child_links' ]
    
    disableIndexesSql = []
    bulkCopySql = []
    postCopySql = []
    
    straightCopyTargets = (
        'history',
        'attachments',
        'ratl_replicas',
        'attachments_blob',
        'parent_child_links',
    )
    dstPrefix = destSession.getTablePrefix()
    dstDb = destSession.db()
    
    (userEntityDefId, groupsFieldDefId) = \
        dstDb.selectSingle(\
            "SELECT e.id, f.id FROM fielddef f, entitydef e " \
            "WHERE e.name = 'users' AND f.name = 'groups' AND " \
            "f.entitydef_id = e.id")[0]
    
    
    sessionCounter = itertools.count(1)
    for sourceSession in sourceSessions:
        sessionCount = sessionCounter.next()
        emptyDb = True if sessionCount == 1 else False
        if not emptyDb:
            dbidOffset = dbidOffsets[sessionCount-2]
            
        sourceReplicaId = str(sourceSession.getReplicaId())
        
        for target in targets:
            if target in straightCopyTargets:
                straightCopy = True
                tableName = target
                entityDef = None
                entityDefName = None
            else:
                straightCopy = False
                entityDef = destSession.GetEntityDef(target)
                entityDefName = entityDef.GetName()
                tableName = entityDef.GetDbName()
            
            dstTable = '.'.join((dstPrefix, tableName))
            srcPrefix = sourceSession.getTablePrefix()
            srcTable = '.'.join((srcPrefix, tableName))
            
            if emptyDb:
                disableIndexesSql += [
                    'ALTER INDEX %s ON %s DISABLE' % (index, dstTable)
                        for index in dstDb.getIndexes(tableName)
                ]
            
            exclude = (
                'dbid',
                'version', 
                'ratl_keysite',
                'lock_version', 
                'locked_by'
            )
            columns = [
                (c[3], 'src.%s' % c[3])
                    for c in dstDb.cursor().columns(table=tableName).fetchall()
                        if c[3] not in exclude
            ]
            
            
            if target in ('attachments_blob', 'parent_child_links'):
                orderBy = ''
                dbidColumn = ''
                where = ''
                if target == 'parent_child_links':
                    where = "WHERE NOT (src.parent_entitydef_id = %d AND "     \
                            "src.child_entitydef_id = %d AND "                 \
                            "src.parent_fielddef_id = %d AND "                 \
                            "src.child_fielddef_id = 0 AND "                   \
                            "src.link_type_enum = 1)" %                        \
                                (userEntityDefId,                              \
                                 userEntityDefId,                              \
                                 groupsFieldDefId)
                    
            else:
                where = 'WHERE src.dbid <> 0'
                #orderBy = 'ORDER BY src.dbid ASC'
                
                orderBy = ''
                if not emptyDb:
                    dbidColumn = '(src.dbid + %s)' % dbidOffset
                else:
                    dbidColumn = 'src.dbid'
                if not straightCopy and \
                       entityDef.GetType()==EntityType.Stateless:
                    where += ' AND NOT EXISTS (%s)' % \
                             entityDef                \
                                .getUniqueKey()       \
                                ._lookupDbIdFromForeignSessionSql('src.dbid',
                                                                  sourceSession)
                
            if target in ('attachments_blob', 'parent_child_links'):
                addReplicaColumn = True
                columns += [ ('ratl_mastership', sourceReplicaId) ]
            else:
                addReplicaColumn = False
                columns += [ ('dbid', dbidColumn) ]
            
            dstColumns = [ c[0] for c in columns ]
            srcColumns = [ c[1] for c in columns ] 
            
            kwds = {
                'where'    : where,
                'orderBy'  : orderBy,
                'srcTable' : srcTable,
                'dstTable' : dstTable,
                'session'  : destSession,
                'tableName': tableName,
                'srcColumns' : ',\n    '.join(srcColumns),
                'dstColumns' : ',\n    '.join(dstColumns),
                'addReplicaColumn' : addReplicaColumn,
            }
            bulkCopySql.append(findSql('bulkCopy', **kwds))
            
            if straightCopy or entityDefName in ('users', 'groups'):
                continue

            isValidReference = lambda f:                                       \
                entityDef.GetFieldDefType(f) in FieldType.referenceTypes and   \
                f not in ('ratl_keysite', 'ratl_mastership') and               \
                (False if emptyDb and entityDef.GetFieldReferenceEntityDef(f)  \
                                               .GetName() not in               \
                                               ('users', 'groups') else True)
                        
            referenceFields = dstDb.selectAll(                      \
                "SELECT f.name, f.db_name,  FROM fielddef f, entitydef e "       \
                "WHERE e.name = '%s' AND f.entitydef_id = e.id "    \
                "AND f.ref_role = 1 AND f.name IN (%s)" %           \
                   (entityDefName, ", ".join([
                        "'%s'" % f for f in entityDef.GetFieldDefNames()
                            if isValidReference(f, FieldType.ReferenceList)
                    ]))
            )
            lookupNewDbId = lambda f, r:                                       \
                r.isStateful() and '%s + %s' % (r.getFieldDbName(f),dbidOffset)\
                or r.getUniqueKey()._lookupDbIdFromForeignSessionSql(          \
                    r.getFieldDbName(f), sourceSession)
                
            referenceFields = dict([
                (f, lookupNewDbId(f, r)) for (f, r) in
                    entityDef.getAllEntityDefsForReferenceFields().items()
                        if f not in ('ratl_keysite', 'ratl_mastership') and not
                        (emptyDb and r.GetName() not in ('users', 'groups'))
            ])
            
            updates = dict()
            for f in referenceFields:
                column = entityDef.getFieldDbName(f)
                ref = entityDef.GetFieldReferenceEntityDef(f)
                
                if ref.GetType() == EntityType.Stateful:
                    newDbId = '%s + %s' % (column, dbidOffset)
                else:
                    newDbId = \
                        ref.getUniqueKey()._lookupDbIdFromForeignSessionSql(
                            column, sourceSession)
                        
                updates[column] = '(CASE WHEN %s = 0 THEN 0 ELSE (%s) END)' %  \
                                   (column, newDbId)
                             
            if updates:
                postCopySql.append(                                 \
                    "UPDATE %s SET %s WHERE ratl_mastership = %s" % \
                    (dstTable,
                     ', '.join(['%s = %s' % i for i in updates.items() ]),
                     sourceReplicaId)
                )
                
#                updates = [ '%s = %s' % i for i in updates.items() ]                             
#                
#                postCopySql.append(findSql('bulkCopy.updateReferences', **{
#                    'updates'   : ',\n    '.join(updates),
#                    'replicaId' : sourceReplicaId,
#                    'session'   : destSession,
#                    'dstTable'  : dstTable,
#                }))
            
            referenceListFields1 = dstDb.selectAll(                  \
                "SELECT f.name FROM fielddef f, entitydef e "       \
                "WHERE e.name = '%s' AND f.entitydef_id = e.id "    \
                "AND f.ref_role = 1 AND f.name IN (%s)" %           \
                   (entityDefName, ", ".join([
                        "'%s'" % f for f in entityDef.GetFieldDefNames()
                            if isValidReference(f, FieldType.ReferenceList)
                    ]))
            )
                        
            referenceListFields2 = lambda entityDef: dstDb.selectAll(                  \
                "SELECT f.name FROM fielddef f, entitydef e "       \
                "WHERE e.name = '%s' AND f.entitydef_id = e.id "    \
                "AND f.ref_role = 1 AND f.name IN (%s)" %           \
                   (entityDefName, ", ".join([
                        "'%s'" % f for f in entityDef.GetFieldDefNames()
                            if isValidReference(f, FieldType.ReferenceList)
                    ]))
            )
                
    return (disableIndexesSql, bulkCopySql, postCopySql)


#===============================================================================
# Classes
#=============================================================================== 

class MergeManager(TaskManager):
    def __init__(self, manager):
        Task.__init__(self, manager)
        self.defaultConfigSection = 'DEFAULT' \
            if len(sys.argv) == 1             \
            else sys.argv[1]
            
            
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

class BulkCopyTask(MergeTask):
    
    def getSourceSessions(self):
        raise NotImplementedError
    
    def run(self):
        sql = []
        destSession = self.destSession
        sourceSessions = self.getSourceSessions()
        sessionCounter = itertools.count(1)
        dbidOffsets = self.manager.conf.get('dbidOffsets').split(',')
        targets = [ e.GetName() for e in destSession.getAllEntityDefs() ] + \
                  [ 'attachments_blob', 'parent_child_links' ]
        
        for sourceSession in sourceSessions:
            sessionCount = sessionCounter.next()
            emptyDb = True if sessionCount == 1 else False
            if not emptyDb:
                dbidOffset = dbidOffsets.pop(0)
            for target in targets:
                sql.append(bulkCopy(destSession,
                                    sourceSession,
                                    target,
                                    emptyDb=emptyDb,
                                    dbidOffset=dbidOffset)[0])
