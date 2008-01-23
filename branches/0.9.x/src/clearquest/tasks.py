"""
clearquest.tasks: module for simplifying ClearQuest migrations.
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
__copyright__ = 'Copyright 2008 OnResolve Ltd'

#===============================================================================
# Decorators
#===============================================================================

#===============================================================================
# Helper Methods
#===============================================================================

def joinPath(*args):
    return os.path.normpath(os.path.join(*args))

def copyUsers(src, dst):
    users = src.Users
    failed = []
    startTime = time.time()
    for user in users:
        try:
            dst.createUserFromXml(user.toXml())
        except:
            failed.append(user.Name)
    stopTime = time.time()
    created = len(users) - len(failed)
    print "created %d users in %s seconds\nfailed: %s" % \
          (created, str(stopTime - startTime), ", ".join(failed))

def spliceWork(data, nchunks):
    size = len(data) / nchunks
    results = []
    results.append((0, size))
    for i in range(1, nchunks-1):
        results.append((i*size, (i+1)*size))
    results.append(((nchunks-1)*size, len(data)-1))
    return results

#===============================================================================
# Classes
#===============================================================================
    
class Dict(dict):
    """
    Helper class that allows access to dictionary items via the normal subscript
    fashion (i.e. tmp['foo']), as well as if the keys were also attributes (i.e.
    tmp.foo).
    """
    def __getattr__(self, name):
        return self.__getitem__(name)
    def __setattr__(self, name, value):
        return self.__setitem__(name, value)
        
class Config(ConfigParser):
    """
    Base configuration class that provides a slightly customised ConfigParser
    interface.  Inherited by TaskManagerConfig.
    """
    def __init__(self, file):
        ConfigParser.__init__(self)
        self.file = file
        self.readfp(open(self.file))
        self.default = self.defaults()
        
        """
        Dictionary interface to the configuration file; keyed by the name of the
        section, the corresponding value will be another dictionary with that
        section's key/value pairs.
        """
        self.data = Dict([(s, Dict(self.items(s))) for s in self.sections()])                                                  
        
    def optionxform(self, option):
        """
        Default implementation of ConfigParser.optionxform() converts options
        to lowercase, which we don't want, so override and return the exact name
        that was passed in.
        """
        return option
    
    def __getitem__(self, name):
        return self.data[name]
    
class TaskManagerConfig(Config):
    def __init__(self, module):
        path = dirname(module.__file__)
        base = basename(module.__file__)
        file = joinPath(path, base[:base.rfind('.')] + '.ini')
        Config.__init__(self, file) 
        
        try:
            self.sourceTarget, self.destTarget = sys.argv[1].split(',')
        except IndexError:
            self.sourceTarget, self.destTarget = self.default['targets'] \
                                                     .split(',')
        
        tasks = self.get('tasks').split(',')
        exclude = self.get('excludeTasks').split(',')
        
        self.tasks = [ t for t in tasks if not t in exclude ]
    
    def get(self, setting, default=None):
        """
        Retrieve the value of @param setting from the destination target's
        configuration file section, if it's present, or from the [DEFAULT]
        section if not.  If the setting isn't present in [DEFAULT] and @param
        default is None, then a ConfigParser.NoOptionError is raised, otherwise
        @param default is returned.
        """
        try:
            return Config.get(self, self.destTarget, setting)
        except NoOptionError, error:
            if default is None:
                raise error
            else:
                return default

class TaskManager(object):
    def __init__(self, module):
        self.module = module
        self.conf = TaskManagerConfig(module)
        
        self.tasks = [ getattr(module, t) for t in self.conf.tasks ]
        self.task = dict()
        
        callbackType = self.conf.get('callback', 'ConsoleCallback')
        self.cb = getattr(callback, callbackType)(self)
        
    def run(self):
        upgradeDb = False
        for task in self.tasks:
            t = task(self)
            t.run()
            # Keep a copy of the task so other tasks can access it.
            self.task[t.__class__.__name__] = t
            
            # Any schema tasks will require the affected destination database to
            # be upgraded in order for the changes to be applied at the end of
            # the task run.
            if isinstance(t, CreateSchemaObjectTask):
                upgradeDb = True
        
        if upgradeDb:
            adminSession = self.getDestSession(api.SessionClassType.Admin)
            dbs = adminSession.Databases
            for db in dbs:
                if db.Name == self.getDestConf().get('db'):
                    self.cb.write("upgrading database '%s'..." % db.Name)
                    start = time.time()
                    db.UpgradeMasterUserInfo()
                    self.cb.write("done (%.3f secs)\n" % (time.time() - start))
                    break
    
    @api.cache
    def getSourceConf(self):
        return self.conf[self.conf.sourceTarget]
    
    @api.cache
    def getDestConf(self):
        return self.conf[self.conf.destTarget]
    
    def _getSession(self, sessionClassType, conf):
        if sessionClassType == api.SessionClassType.User:
            fmt = "Logging on to user database %(db)s [%(dbset)s] as "
        else:
            fmt = "Logging on to schema %(dbset)s as "
        fmt += "%(login)s/%(passwd)s..."
        self.cb.write(fmt % conf)
        start = time.time()
        session = api.getSession(sessionClassType, conf)
        self.cb.write("done (%.3f secs)\n" % (time.time() - start))
        return session
    
    @api.cache
    def getSourceSession(self, sessionClassType):
        return self._getSession(sessionClassType, self.getSourceConf())
    
    @api.cache
    def getDestSession(self, sessionClassType):
        return self._getSession(sessionClassType, self.getDestConf())
    
class Task(object):
    def __init__(self, manager):
        self.manager = manager
        self.cb = self.getCallback()
        self.destSession = manager.getDestSession(self.getSessionClassType())
        
        """
        If there's an .ini file lying around with the same name as this class,
        assume it's a data configuration file (which can store various mappings
        and other values of interest to subclasses) and make it accessible via
        a Dict interface through self.data.
        """
        self.data = self._data()
    
    def getSessionClassType(self):
        """
        @return: a value corresponding to api.SessionClassType (either User or
        Admin).  Must be implemented by subclass.
        """
        raise NotImplementedError
    
    def getCallback(self):
        """
        @returns: an object that implements the Callback interface.  Defaults
        to whatever callback class the parent TaskManager is using.
        """
        return self.manager.cb.__class__(self)
    
    def run(self):
        pass
    
    def _data(self):
        name = self.__class__.__name__.lower()
        file = joinPath(dirname(self.manager.module.__file__), name + '.ini')
        if os.path.isfile(file):
            self.conf = Config(file)
            return self.conf.data

class CreateObjectTask(Task):
    def __init__(self, manager):
        Task.__init__(self, manager)
        
    def expectedCount(self):
        """
        @returns: the number of destination objects that will be created.  Used
        by the callback mechanism to track progress.
        """
        raise NotImplementedError
    
    def getDestObjectNames(self):
        """
        @returns: a list of object names to create/update.
        """
        raise NotImplementedError
    
    def getDestObject(self, destObjectName):
        """
        @return: a two-part tuple, the first element containing an instance of 
        an api.CQProxyObject, configured with a behaviour type that implements
        api.DeferredWriteBehaviour, the second element being a boolean that is
        True if the entity was created from scratch, or False if it already
        existed and a reference was obtained.
        """
        raise NotImplementedError
    
    def preCommit(self, destObject, created):
        """
        Callback that's invoked when the entity is in an editable state, but
        before it's been committed.  Provides an opportunity for a subclass to
        customise the entity's values before being commited.
        @param destEntity: instance of api.DeferredWriteEntityProxy
        @param created: True if destObject was created, False if it already
        existed
        """
        pass
    
    def postCommit(self, destObject, changesApplied):
        """
        Callback that's invoked after successfully committing the entity.  If an
        exception was raised during validation or commit, this callback will not
        be invoked.
        @param destObject: instance of api.DeferredWriteEntityProxy
        @param changesApplied: bool indicating whether or not any changes were
        actually applied to destEntity.  If no unique changes are detected by
        DeferredWriteEntityProxy.applyChangesAndCommit() for a destEntity that
        already existed, it won't bother with attempting to modify then commit
        the entity.
        """
        pass
    
    def run(self):
        """
        For each object name returned by getDestObjectNames(), get a reference
        to the object via getDestObject(), call preCommit() to allow the
        subclass to customise values, call applyChangesAndCommit() against the
        object, then, assuming no exceptions were thrown, call postCommit(),
        allowing for the subclass to carry out any additional tasks once the
        object's been created.
        """
        cb = self.cb
        cb.expected = self.expectedCount()
        for destObjectName in self.getDestObjectNames():
            cb.next(destObjectName)
            try:
                destObject, created = self.getDestObject(destObjectName)
                counter = 'created' if created else 'updated'
               
                self.preCommit(destObject, created)
                
                try:
                    changesApplied = destObject.applyChangesAndCommit()
                except:
                    destObject.revert()
                    raise
                
                if not changesApplied:
                    counter = 'skipped'
                cb[counter] += 1
                
                self.postCommit(destObject, changesApplied)
                
            except Exception, details:
                cb.error((details, traceback.format_exc()))
                cb.failed += 1
                
        cb.finished()
    
    
class CreateSchemaObjectTask(CreateObjectTask):
    def __init__(self, manager, schemaObjectClass):
        self._schemaObjectClass = schemaObjectClass
        CreateObjectTask.__init__(self, manager)
        
    def getSessionClassType(self):
        return api.SessionClassType.Admin
    
    def getDestObject(self, destObjectName):
        """
        @param destObjectName: name of the schema object being created
        @return: instance of an api.DeferredWriteSchemaObjectProxy object
        """
        destObject = None
        className = self._schemaObjectClass.__name__
        get = getattr(self.destSession, 'Get' + className)
        create = getattr(self.destSession, 'Create' + className)
        try:
            destObject = get(destObjectName)
            created = False
        except:
            pass
        
        if destObject is None:
            destObject = create(destObjectName)
            created = True
        
        return (api.DeferredWriteSchemaObjectProxy(destObject), created)

class CreateEntityTask(CreateObjectTask):
    """
    Creates (or updates) a new entity.
    """
    def getSessionClassType(self):
        return api.SessionClassType.User
    
    def getDestEntityDefName(self):
        """
        @returns: name of the entity to create/update.
        """
        raise NotImplementedError
    
    @api.cache
    def getDestEntityDef(self):
        """
        @returns: instance of an api.EntityDef object for the destination entity
        name returned by destEntityDefName().
        """
        return self.destSession.GetEntityDef(self.getDestEntityDefName())
    
    def getDestObject(self, destDisplayName):
        """
        @param destDisplayName: new entity's display name.
        @returns: instance of an api.DeferredWriteEntityProxy for the entity
        identified by @param destDisplayName. If no existing entity can be found
        matching the display name, then a new one is created via BuildEntity.
        Note: for stateless entities with complex unique keys (more than one
        field), if the entity doesn't already exist and has to be created with 
        BuildEntity, this method will make a crude attempt to 'prime' the new
        entity's display name (that is, set each individual field) with the
        relevant values.  This can only be done if each field in the unique key
        doesn't contain a space, given that ClearQuest uses spaces to separate
        each fields.  So, the number of spaces found in the display name must
        be one less than the number of fields in the destEntity's unique key.
        If this isn't the case, a ValueError will be raised.  (We'll need to
        change this down the track, perhaps by adding a setDestDisplayName()
        method that subclasses can override.)
        """
        session = self.destSession
        destEntityDefName = self.getDestEntityDefName()
        destEntityDef = self.getDestEntityDef()
        
        fields = [ f[0] for f in destEntityDef.getUniqueKey().fields() ]
        if len(fields) == 1:
            parts = (destDisplayName,)
        else:
            parts = destDisplayName.split(' ')
            
        if len(parts) != len(fields):
            raise ValueError, "could not discern unique key parts (%s) " \
                              "for entity '%s' from display name '%s'" % \
                              (", ".join(fields), \
                               destEntityDefName, \
                               destDisplayName)
        else:
            changes = dict([(f, v) for f, v in zip(fields, parts)])
        
        args = (destEntityDefName, destDisplayName)
        dbid = session.lookupEntityDbIdByDisplayName(*args)
        if dbid:
            entity = session.GetEntityByDbId(destEntityDefName, dbid)
            created = False
        else:
            entity = session.BuildEntity(destEntityDefName)
            created = True
        
        return (api.DeferredWriteEntityProxy(entity, changes=changes), created)
    
class CopyObjectTask(CreateObjectTask):
    def __init__(self, manager):
        CreateObjectTask.__init__(self, manager)
        self.sourceSession=manager.getSourceSession(self.getSessionClassType())
    
    def getSourceObjectName(self, destObjectName):
        """
        Given a destination object name, return the corresponding object name
        of the source object we're copying.  Called by getSourceObject() in
        order to get a reference to the source object before copying.
        @returns: unless overridden by a subclass, this method simply returns
        destObjectName.
        """
        return destObjectName

    def getSourceObject(self, sourceObjectName):
        """
        @param sourceObjectName: source object's name
        @returns: instance of an api.CQProxyObject configured with the behaviour
        api.ReadOnlyBehaviour.
        """
        raise NotImplementedError
    
    def skipObject(self, sourceObject):
        """
        Callback that's invoked after the sourceObject has been created, but
        before the destObject has been created.  Provides an opportunity for a
        subclass to indicate if the sourceObject should be 'skipped over'.
        @return: True if the object should be skipped (no destObject will be
        created), False otherwise.  Default is False.
        """
        return False
    
    def preCommit(self, sourceObject, destObject, created):
        """
        Callback that's invoked when the entity is in an editable state, but
        before it's been committed.  Provides an opportunity for a subclass to
        customise the entity's values before being commited.
        @param sourceEntity: instance of api.ReadOnlyEntityProxy
        @param destEntity: instance of api.DeferredWriteEntityProxy
        @param created: True if destObject was created, False if it already
        existed
        @return: True if the the current copy operation should continue, False
        if creation/modification of this destObject should be skipped.
        """
        pass
    
    def postCommit(self, sourceObject, destObject, changesApplied):
        """
        Callback that's invoked after successfully committing the entity.  If an
        exception was raised during validation or commit, this callback will not
        be invoked.
        @param sourceEntity: instance of api.ReadOnlyEntityProxy
        @param destEntity: instance of api.DeferredWriteEntityProxy
        @param changesApplied: bool indicating whether or not any changes were
        actually applied to destEntity.  If no unique changes are detected by
        DeferredWriteEntityProxy.applyChangesAndCommit() for a destEntity that
        already existed, it won't bother with attempting to modify then commit
        the entity.
        """
        pass
    
    def commonFieldNames(self, sourceObject, destObject):
        """
        @param sourceObject: instance of api.ReadOnlyObjectProxy
        @param destEntity: instance of api.DeferredWriteObjectProxy
        @returns: list of identically named fields shared by sourceObject and 
        destObject.  copyCommonFields() is the intended consumer of this method.
        """
        pass
    
    def copyCommonFields(self, sourceObject, destObject, skipFields=list()):
        """
        Convenience method that copies any identical fields in the source object
        to the destination object.  See subclasses for additional information.
        """
        fields = self.commonFieldNames(sourceObject, destObject)
        getter = repeat(lambda f: sourceObject.get(f))
        destObject.addChanges(dict([
            (f, v) for (f, v) in [
                (f, v(f)) for (f, v) in zip(fields, getter)
                    if not f in skipFields
            ] if v
        ]))
    
    def run(self):
        """
        For each display name returned by destDisplayNames(), get a reference to
        the new dest entity (either via BuildEntity if it doesn't exist, or 
        GetEntity if it does) as well as the source entity, call preCommit()
        with both sourceEntity and destEntity, allowing subclass customisation,
        then validate and commit the destEntity if any changes were made, and
        then finally call postCommit() with both entities again allowing the
        subclass to perform any final tasks.  Called by TaskManager.  The logic
        in this method is identical to that of CreateEntity.run(), except for
        the support of an additional source entity.
        """
        cb = self.cb
        cb.expected = self.expectedCount()
        for destObjectName in self.getDestObjectNames():
            cb.next(destObjectName)
            try:
                sourceObjectName = self.getSourceObjectName(destObjectName)
                sourceObject = self.getSourceObject(sourceObjectName)
                
                if self.skipObject(sourceObject):
                    cb.skipped += 1
                    continue
                
                destObject, created = self.getDestObject(destObjectName)
                counter = 'created' if created else 'updated'
               
                self.preCommit(sourceObject, destObject, created)
                
                try:
                    changesApplied = destObject.applyChangesAndCommit()
                except:
                    destObject.revert()
                    raise
                
                if not changesApplied:
                    counter = 'skipped'
                cb[counter] += 1
                
                self.postCommit(sourceObject, destObject, changesApplied)
                
            except Exception, details:
                cb.error((details, traceback.format_exc()))
                cb.failed += 1
        
        cb.finished()
    
class CopySchemaObjectTask(CopyObjectTask, CreateSchemaObjectTask):
    def __init__(self, manager, schemaObjectClass):
        self._schemaObjectClass = schemaObjectClass
        CopyObjectTask.__init__(self, manager)
        
        collectionName = schemaObjectClass.__name__ + 's'
        self._sourceObjects = getattr(self.sourceSession, collectionName)
        
    def expectedCount(self):
        return len(self._sourceObjects)
    
    def getDestObjectNames(self):
        for sourceObject in self._sourceObjects:
            self._currentSourceObject = sourceObject
            yield sourceObject.Name        
    
    def getSourceObject(self, sourceObjectName):
        """
        @param sourceObjectName: source object's name
        @returns: instance of an api.CQProxyObject configured with the behaviour
        api.ReadOnlyBehaviour.
        """
        assert(sourceObjectName == self._currentSourceObject.Name)
        return api.ReadOnlySchemaObjectProxy(self._currentSourceObject)
    
    def commonFieldNames(self, sourceObject, destObject):
        """
        @return: list of properties for the given schema object (taken from
        the keys of the schema object's _prop_map_get_ dict).
        """
        assert(sourceObject._proxiedObject.__class__.__name__ ==
                 destObject._proxiedObject.__class__.__name__)
        return sourceObject._proxiedObject._prop_map_get_.keys()
        
class CopyEntityTask(CopyObjectTask, CreateEntityTask):
    """
    Similar to CreateEntityTask, except that our destination entity is created
    from the values in a source entity.  
    """
    def __init__(self, manager):
        CopyObjectTask.__init__(self, manager)
        
    def getSourceEntityDefName(self):
        """
        @returns: name of the source entity being copied, must be implemented by
        subclass.
        """
        raise NotImplementedError
    
    @api.cache
    def getSourceEntityDef(self):
        """
        @returns: instance of an api.EntityDef object for the source entity
        name returned by getSourceEntityDefName().
        """
        return self.sourceSession.GetEntityDef(self.getSourceEntityDefName())
    
    def getSourceObject(self, sourceDisplayName):
        """
        @param sourceDisplayName: source entity's display name as a string
        @returns: instance of an api.ReadOnlyEntityProxy for the entity
        identified by @param sourceDisplayName.
        """
        s = self.sourceSession
        n = self.getSourceEntityDefName()
        return api.ReadOnlyEntityProxy(s.GetEntity(n, sourceDisplayName))

    
    @api.cache
    def commonFieldNames(self, sourceEntity, destEntity):
        """
        @param sourceEntity: instance of api.ReadOnlyEntityProxy
        @param destEntity: instance of api.DeferredWriteEntityProxy
        @returns: list of identically named fields shared by sourceEntity and 
        destEntity, excluding any system owned fields.  copyCommonFields() is
        the intended consumer of this method.
        """
        sourceEntityDef = sourceEntity.getEntityDef()
        destEntityDef = destEntity.getEntityDef()
        loweredFields = [ f.lower() for f in destEntityDef.GetFieldDefNames() ]
        destFields = api.listToMap(loweredFields)
        return [
            f for f in [ n.lower() for n in sourceEntityDef.GetFieldDefNames() ]
                if not sourceEntityDef.IsSystemOwnedFieldDefName(f) and
                       f in destFields
        ]

class CreateUserTask(CreateSchemaObjectTask):
    def __init__(self, manager):
        CreateSchemaObjectTask.__init__(self, manager, api.User)

class CreateGroupTask(CreateSchemaObjectTask):
    def __init__(self, manager):
        CreateSchemaObjectTask.__init__(self, manager, api.Group)

class CreateDatabaseTask(CreateSchemaObjectTask):
    def __init__(self, manager):
        CreateSchemaObjectTask.__init__(self, manager, api.Database)

class CopyUserTask(CopySchemaObjectTask):
    def __init__(self, manager):
        CopySchemaObjectTask.__init__(self, manager, api.User)

class CopyGroupTask(CopySchemaObjectTask):
    def __init__(self, manager):
        CopySchemaObjectTask.__init__(self, manager, api.Group)

class CopyDatabaseTask(CopySchemaObjectTask):
    def __init__(self, manager):
        CopySchemaObjectTask.__init__(self, manager, api.Database)

class UpdateDynamicListsTask(Task):
    def __init__(self, manager):
        Task.__init__(self, manager)
    
    def getSessionClassType(self):
        return api.SessionClassType.User
        
    def getXmlFileName(self):
        name = self.__class__.__name__.lower()
        return joinPath(dirname(self.manager.module.__file__), name + '.xml')
    
    def loadXml(self):
        return XML(open(self.getXmlFileName(), 'r').read())
    
    def run(self):
        cb = self.cb
        xml = self.loadXml()
        dynamicLists = api.loadDynamicLists(xml)
        cb.expected = len(dynamicLists)
        for dl in dynamicLists:
            name = dl.Name
            cb.next(name)
            try:
                old = list(self.destSession.getDynamicList(name).values or [])
                new = list(self.destSession.updateDynamicList(dl).values or [])
                old.sort()
                new.sort()
                if old != new:
                    cb.updated += 1
                else:
                    cb.skipped += 1
            except Exception, details:
                cb.error((details, traceback.format_exc()))
                cb.failed += 1                
        cb.finished()

