
import dbi
import pyodbc as odbc
from os.path import dirname, isfile
from genshi.template import TemplateLoader, TextTemplate
from clearquest.util import joinPath

_SqlTemplateDir = joinPath(dirname(__file__), 'sql')
_SqlLoader = TemplateLoader(_SqlTemplateDir, 
                            default_class=TextTemplate,
                            auto_reload=True)

class DatabaseError(Exception):
    """
    Helper class for wrapping any DBI error we may encounter in a single class.
    """
    def __init__(self, details, sql):
        self.args = (details, "Offending SQL:\n%s" % sql)
        


def _findSql(session, classOrModuleName, methodName, *args, **kwds):
    vendor = session.getDbVendorName()
    fileName = '%s.%s.%s.sql' % (classOrModuleName, methodName, vendor)
    if not isfile(joinPath(_SqlTemplateDir, fileName)):
        fileName = '%s.%s.sql' % (classOrModuleName, methodName)
    
    return _SqlLoader.load(fileName) \
                     .generate(args=args, **kwds) \
                     .render('text')
                     
def sql(mf):
    def decorator(f):
        fname = f.func_name
        def _newf(*args, **kwds):
            args = list(args)
            this = None
            self = args[0]
            if self.__class__.__name__ != 'Session':
                classOrModuleName = self.__class__.__name__
                this = self
                session = self.session
            else:
                session = self
                classOrModuleName = f.__module__
                
            m = f(*args, **kwds)
            if isinstance(m, dict):
                kwds.update(m)
            elif isinstance(m, (list, tuple)):
                args += m
            elif m is not None:
                args += (m,)
                        
            kwds['this'] = this
            sql = _findSql(session, classOrModuleName, fname, *args, **kwds)
            return getattr(session.db(), mf.func_name)(sql, *args[1:])
        return _newf
    return decorator

@sql
def select(f): pass

@sql
def selectSingle(f): pass

@sql
def selectAll(f): pass

@sql
def execute(f): pass

class Connection(object):
    def __init__(self, connectString):
        self._connectString = connectString
        self._con = odbc.connect(connectString)
    
    def _execute(self, sql, *args):
        cursor = self._con.cursor()
        try:
            cursor.execute(sql, *args)
        except (dbi.dataError,
                dbi.integrityError,
                dbi.internalError,
                dbi.opError,
                dbi.progError), details:
            raise DatabaseError(details, sql)
        return cursor
    
    def select(self, sql, *args):
        cursor = self._execute(sql, *args)
        single = len(cursor.description) == 1
        for row in iter(lambda: cursor.fetchone(), None):
            yield row[0] if single else row        
    
    def selectAll(self, sql, *args):
        cursor = self._execute(sql, *args)
        single = len(cursor.description) == 1
        results = cursor.fetchall()
        return results if not single else [ row[0] for row in results ]
    
    def selectSingle(self, sql, *args):
        cursor = self._execute(sql, *args)
        try:
            return cursor.fetchone()[0]
        except TypeError:
            return None
    
    def execute(self, sql, *args):
        if self._execute(sql, *args) == -1:
            raise DatabaseError('execute returned -1', sql)
    
    def __getattr__(self, attr):
        return getattr(self._con, attr)