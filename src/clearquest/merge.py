"""
clearquest.merge: module for merging ClearQuest databases
"""

#===============================================================================
# Imports
#===============================================================================

import sys
import itertools
from itertools import repeat
import cStringIO as StringIO

from pywintypes import com_error

from clearquest import api, db
from clearquest.task import Task, TaskManager
from clearquest.util import connectStringToMap, listToMap
from clearquest.constants import EntityType, FieldType, SessionClassType

#===============================================================================
# Globals
#===============================================================================
__rcsid__ = '$Id$'
__rcsurl__ = '$URL$'
__copyright__ = 'Copyright 2008 OnResolve Ltd'

__orig_db   = 'merge_orig_db'
__orig_id   = 'merge_orig_id'
__orig_dbid = 'merge_orig_dbid'

MergeFields = {
    __orig_db    : FieldType.Integer,
    __orig_dbid  : FieldType.Integer,
}

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

def addMergeFields(adminSession, destSession):
    """
    @param adminSession: instance of api.AdminSession() that's logged in to the
    schema database that the merge fields should be added to.
    @param destSession: instance of api.Session() for any database using the
    schema that is to be modified to add the merge fields.
    """
    adminSession.setVisible(False, destSession._databaseName)
    
    import clearquest.designer
    designer = clearquest.designer.Designer()
    designer.Login(adminSession._databaseName,
                   adminSession._loginName,
                   adminSession._password,
                   adminSession._databaseSet)
    
    schemaName = destSession.schemaName()
    designer.CheckoutSchema(schemaName, '')
    
    for entityDef in destSession.getAllEntityDefs():
        entityDefName = entityDef.GetName()
        fields = listToMap(entityDef.GetFieldDefNames()).keys()
        for (mergeField, mergeFieldType) in MergeFields.items():
            if mergeField not in fields:
                print "adding field '%s' of type '%s' to entity '%s'..." % (   \
                    mergeField,
                    FieldType[mergeFieldType],
                    entityDefName
                )
                designer.UpgradeFieldDef(entityDefName,
                                         mergeField,
                                         mergeFieldType,
                                         None)
            else:
                if entityDef.GetFieldDefType(mergeField) != mergeFieldType:
                    raise RuntimeError("entity '%s' already has a field named "
                                       "'%', but it is not a short string." %  \
                                       (entityDefName, mergeField))
                
            oldMergeField = '__%s' % mergeField
            if oldMergeField in fields:
                print "deleting old field '%s' from entity '%s'..." % (   \
                    oldMergeField,
                    entityDefName
                )
                designer.DeleteFieldDef(entityDefName, oldMergeField)
                
    print "validating schema..."
    designer.ValidateSchema()
    print "checking in schema..."
    designer.CheckinSchema('Added fields for database merge.')
    print "upgrading database '%s'..." % destSession._databaseName
    designer.UpgradeDatabase(destSession._databaseName)
    designer.Logoff()
    del designer
    
    adminSession.setVisible(True, destSession._databaseName)

def getRecommendedStatefulDbIdOffset(session):
    maximum = (0, '<entity def name>')
    dbc = session.db()
    for entityDef in session.getStatefulEntityDefs():
        name = entityDef.GetDbName()
        m = dbc.selectSingle('SELECT MAX(dbid) FROM %s' % name)
        if m > maximum[0]:
            maximum = (m, name)
    
    # 0x2000000 = 33554432: starting dbid used by CQ databases.  The
    # offset gets added to each dbid, which, technically, could be as
    # low as the very minimum, so check that the dbidOffset provided is
    # sufficient.
    if maximum[0] == 0:
        return 0
    m = str(maximum[0] - 33554432)
    recommended = str(int(m[0])+1) + '0' * (len(m)-1)
    return int(recommended)

def getMaxIdForField(session, table, column):
    db = session.db().selectSingle('SELECT MAX(%s) FROM %s' % (column, table))

def getDbIdOffsets(destSession, session):
    dbidOffsets = dict()
    for table in getTargets(api.EntityDef.GetDbName):
        if table in ('attachments_blob', 'ratl_replicas'):
            continue
        offset = dstDb.selectSingle('SELECT MAX(dbid) FROM %s' % table)
        if not offset:
            offset = 0
        if offset > 0:
            offset -= 33554432
        dbidOffsets[table] = offset
    
    statefulDbIdOffset = getRecommendedStatefulDbIdOffset(destSession)
    dbidOffsets.update(
        zip([e.GetDbName() for e in destSession.getStatefulEntityDefs()], 
            repeat(statefulDbIdOffset))
    )

def bulkCopy(destSession, sourceSessions, output=StringIO.StringIO(),
             mergeFields=MergeFields):
    
    if mergeFields:
        # Every entityDef in destSession should have all merge fields.
        for entityDef in destSession.getAllEntityDefs():
            fields = listToMap(entityDef.GetFieldDefNames())
            for expected in mergeFields.keys():
                if expected not in fields:
                    raise RuntimeError("entity '%s' is missing field '%s', "   \
                                       "run addMergeFields() first" %          \
                                       (entityDef.GetName(), expected))
    

    preCopySql  = []
    bulkCopySql = []
    postCopySql = []
    enableIndexesSql = []
    disableIndexesSql = []
    
    dstReplicaId = destSession.getReplicaId()
    dstPrefix = destSession.getTablePrefix()
    dstDb = destSession.db()
    
    straightCopyTargets = (
        u'history',
        u'attachments',
        u'attachments_blob',
        u'parent_child_links',
    )
    getTargets = lambda method: list(straightCopyTargets) + [
        method(e) for e in destSession.getAllEntityDefs()
            if method(e) not in straightCopyTargets
    ]
    targets = getTargets(api.EntityDef.GetName)
    
    dbidOffsets = dict()
    for table in getTargets(api.EntityDef.GetDbName):
        if table in ('attachments_blob', 'ratl_replicas'):
            continue
        offset = dstDb.selectSingle('SELECT MAX(dbid) FROM %s' % table)
        if not offset:
            offset = 0
        if offset > 0:
            offset -= 33554432
        dbidOffsets[table] = offset
    
    statefulDbIdOffset = getRecommendedStatefulDbIdOffset(destSession)
    dbidOffsets.update(
        zip([e.GetDbName() for e in destSession.getStatefulEntityDefs()], 
            repeat(statefulDbIdOffset))
    )
    

    (userEntityDefId, groupsFieldDefId) =                       \
        dstDb.selectAll(                                        \
            "SELECT e.id, f.id FROM fielddef f, entitydef e "   \
            "WHERE e.name = 'users' AND f.name = 'groups' AND " \
            "f.entitydef_id = e.id"                             \
        )[0]
    
    preCopySql.append(findSql('bulkCopy.setPreCopyDatabaseOptions', **{
        'destSession'  : destSession
    }))
    
    emptyDb = False
    firstSession = True
    sessionCounter = itertools.count(1)
    for sourceSession in sourceSessions:
        sessionCount = sessionCounter.next()
        if sessionCount > 1:
            emptyDb = False
            firstSession = False
            previousSession = sourceSessions[sessionCount-2]
            dbidOffsets = dict()
            prevDb = previousSession.db()
            for table in getTargets(api.EntityDef.GetDbName):
                if table in ('attachments_blob', 'ratl_replicas'):
                    continue
                offset = prevDb.selectSingle('SELECT MAX(dbid) FROM %s' % table)
                if not offset:
                    offset = 0
                if offset > 0:
                    offset -= 33554432+1
                dbidOffsets[table] = offset
            
            statefulDbIdOffset = getRecommendedStatefulDbIdOffset(\
                previousSession)
            
            dbidOffsets.update(zip([
                    e.GetDbName() for e in destSession.getStatefulEntityDefs()
                ], repeat(statefulDbIdOffset))
            )
        else:
            firstSession = True
            previousSession = None
            dbidOffsets = dict()
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
                    
        sourceReplicaId = str(sourceSession.getReplicaId())
        
        for target in targets:
            if target in straightCopyTargets:
                straightCopy = True
                tableName = target
                entityDef = None
                entityDefName = None
                entityType = None
                isStatefulEntity= False
            else:
                straightCopy = False
                entityDef = destSession.GetEntityDef(target)
                entityDefName = entityDef.GetName()
                entityType= entityDef.GetType()
                isStatefulEntity = entityDef.GetType() == EntityType.Stateful
                tableName = entityDef.GetDbName()
            
            dstTable = '.'.join((dstPrefix, tableName))
            srcPrefix = sourceSession.getTablePrefix()
            srcTable = '.'.join((srcPrefix, tableName))
            
            dbidOffset = dbidOffsets.get(tableName) or 0
            
            if firstSession:
                disableIndexesSql.append('\nGO\n'.join([
                    'ALTER INDEX %s ON %s DISABLE' % (index, dstTable)
                        for index in dstDb.getIndexes(tableName)
                ]))
                
                enableIndexesSql.append('\nGO\n'.join([
                    'ALTER INDEX %s ON %s REBUILD' % (index, dstTable)
                        for index in dstDb.getIndexes(tableName)
                ]))
                
            exclude = [
                'dbid',
                'ratl_keysite',
                'ratl_mastership',
                'lock_version', 
                'locked_by',
            ]
            if mergeFields:
                exclude += [ key for key in mergeFields.keys() ]
                
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
                orderBy = 'ORDER BY src.dbid ASC'
                
                if emptyDb:
                    dbidColumn = 'src.dbid'
                else:
                    dbidColumn = '(src.dbid + %d)' % dbidOffset
                if not straightCopy and \
                       entityDef.GetType() == EntityType.Stateless:
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
                columns += [ ('dbid', dbidColumn), ]
                if mergeFields:
                    columns += [
                        (__orig_db, "'%s'" % sourceSession._databaseName),
                        (__orig_dbid, 'src.dbid'),
                        (__orig_id, ('src.id' if isStatefulEntity else 'NULL')),
                    ]
            
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
            
            attachmentsDbIdOffset = dbidOffsets['attachments']
            kwds = {
                'dstTable'  	: dstTable,
                'dstPrefix' 	: dstPrefix,
                'dbidOffset'    : dbidOffset,
                'entityDefName' : entityDefName,
                'sourceReplicaId': sourceReplicaId,
                'attachmentsDbIdOffset' : attachmentsDbIdOffset
            }
            postCopySql.append(findSql('bulkCopy.updateHistory', **kwds))
            
            # Update attachments & attachments_blob.
            postCopySql.append(findSql('bulkCopy.updateAttachments', **kwds))
            
            # And finally, update ratl_mastership, indicating that this entity
            # has now been merged completely into the destination database.
            postCopySql.append(                                                \
                "UPDATE %s SET ratl_mastership = %d WHERE dbid <> 0 "          \
                "AND ratl_mastership <> %d" %                                  \
                    (dstTable,
                     dstReplicaId,
                     dstReplicaId)
            )
            
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
                    'attachmentsDbIdOffset' : attachmentsDbIdOffset
                })
            )
            allEntityDefs = destSession.getAllEntityDefs()

    
    getMaxDbIdSql = lambda method:                                             \
        'SELECT TOP 1 ((dbid+1)-0x2000000) FROM (%s) AS x ORDER BY dbid DESC' %\
            ' UNION '.join([                                                   \
                'SELECT MAX(dbid) AS dbid FROM %s' % entityDef.GetDbName()     \
                    for entityDef in method(destSession)                       \
            ])
        
    postCopySql.append(
        'UPDATE %s.dbglobal SET next_request_id = (%s), next_aux_id = (%s)' % (
            dstPrefix,
            getMaxDbIdSql(api.Session.getStatefulEntityDefs),
            getMaxDbIdSql(api.Session.getStatelessEntityDefs)
        )
    )
    
    postCopySql.append(\
        findSql('bulkCopy.setPostCopyDatabaseOptions', **{
            'destSession' : destSession,
        })
    )
    
    output.write('\nGO\n'.join([
        '\nGO\n'.join([
            line for line in section
        ]) for section in (
            disableIndexesSql,
            preCopySql,
            bulkCopySql,
            postCopySql,
            enableIndexesSql
        )
    ]))
    
    return output

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
